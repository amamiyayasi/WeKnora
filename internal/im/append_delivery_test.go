package im

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestWechatAppendUsesOriginalQAAndPersistsFinal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sent []string
		var sentMu sync.Mutex
		snapshot := func() []string { sentMu.Lock(); defer sentMu.Unlock(); return append([]string(nil), sent...) }
		calls := 0
		service := &Service{messageService: &deliveryMessageService{}, streamManager: &fullOutputStreamManager{}, sessionService: &exitSessionService{run: func(ctx context.Context, bus *event.EventBus) error {
			calls++
			require.Equal(t, true, ctx.Value(types.AppendOnlyAnswerContextKey))
			_ = bus.Emit(ctx, event.Event{Type: event.EventAgentFinalAnswer, Data: event.AgentFinalAnswerData{Content: "先查一下（不能当正文）\n\n"}})
			time.Sleep(3 * time.Second)
			require.Empty(t, snapshot())
			_ = bus.Emit(ctx, event.Event{Type: event.EventAgentToolCall, Data: event.AgentToolCallData{ToolCallID: "search", ToolName: "web_search"}})
			_ = bus.Emit(ctx, event.Event{Type: event.EventAgentFinalAnswer, Data: event.AgentFinalAnswerData{Content: "第一段🙂\n\n第二段", Committed: true}})
			time.Sleep(3 * time.Second)
			require.Equal(t, []string{"第一段🙂\n\n"}, snapshot())
			_ = bus.Emit(ctx, event.Event{Type: event.EventAgentComplete, Data: event.AgentCompleteData{FinalAnswer: "第一段🙂\n\n第二段"}})
			return nil
		}}}
		writer := newAppendReply(&IMChannel{}, func(_ context.Context, text string) error {
			sentMu.Lock()
			sent = append(sent, text)
			sentMu.Unlock()
			return nil
		})
		err := service.handleMessageOutput(context.Background(), &IncomingMessage{Platform: PlatformWeChat, UserID: "user", Content: "question"}, &types.Session{ID: "session"}, streamExitAgent(), nil, nil, nil, nil, nil, "user", nil, writer)
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, "第一段🙂\n\n第二段", strings.Join(sent, ""))
		require.Equal(t, "第一段🙂\n\n第二段", service.messageService.(*deliveryMessageService).saved.Content)
	})
}

func TestWechatAppendUnknownSendNeverReplaysFinalOrRunsQATwice(t *testing.T) {
	service, _, _, _ := newFullOutputHarness("完整回答")
	service.messageService = &deliveryMessageService{}
	attempts := 0
	writer := newAppendReply(&IMChannel{}, func(context.Context, string) error { attempts++; return errors.New("unknown delivery") })
	err := service.handleMessageOutput(context.Background(), &IncomingMessage{Platform: PlatformWeChat, UserID: "user", Content: "question"}, &types.Session{ID: "session"}, nil, nil, nil, nil, nil, nil, "user", nil, writer)
	require.ErrorContains(t, err, "unknown delivery")
	require.Equal(t, 1, attempts)
	require.Equal(t, "完整回答", service.messageService.(*deliveryMessageService).saved.Content)
}

func TestWechatAppendErrorStopsQueuedText(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		var sent []string
		service := &Service{messageService: &deliveryMessageService{}, streamManager: &fullOutputStreamManager{}, sessionService: &exitSessionService{run: func(ctx context.Context, bus *event.EventBus) error {
			_ = bus.Emit(ctx, event.Event{Type: event.EventAgentFinalAnswer, Data: event.AgentFinalAnswerData{Content: strings.Repeat("待发送", 1600), Committed: true}})
			<-started
			_ = bus.Emit(ctx, event.Event{Type: event.EventError, Data: event.ErrorData{Error: "provider failed"}})
			return nil
		}}}
		first := true
		writer := newAppendReply(&IMChannel{}, func(ctx context.Context, text string) error {
			if first {
				first = false
				close(started)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(3 * time.Second):
				}
			}
			sent = append(sent, text)
			return nil
		})
		err := service.handleMessageOutput(context.Background(), &IncomingMessage{Platform: PlatformWeChat, UserID: "user"}, &types.Session{ID: "session"}, streamExitAgent(), nil, nil, nil, nil, nil, "user", nil, writer)
		require.NoError(t, err)
		require.Equal(t, []string{imErrorFallback}, sent)
	})
}

type cancellableFullReplyAdapter struct {
	Adapter
	cancel      context.CancelFunc
	outboundErr error
}

func (a *cancellableFullReplyAdapter) SendChunk(context.Context, *IncomingMessage, string) error {
	return nil
}
func (a *cancellableFullReplyAdapter) SendReply(ctx context.Context, _ *IncomingMessage, _ *ReplyMessage) error {
	a.cancel()
	a.outboundErr = ctx.Err()
	return a.outboundErr
}

func TestWechatFullReplyKeepsCancellationDuringLongDelivery(t *testing.T) {
	service, _, _, _ := newFullOutputHarness("完整正文")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := &cancellableFullReplyAdapter{cancel: cancel}
	service.executeQARequest(&qaRequest{ctx: ctx, cancel: cancel, userKey: "user", adapter: adapter,
		msg: &IncomingMessage{Platform: PlatformWeChat, UserID: "user", Content: "question"}, session: &types.Session{ID: "session"}, channel: &IMChannel{OutputMode: "full"}})
	require.ErrorIs(t, adapter.outboundErr, context.Canceled)
}

type appendPhaseChat struct {
	calls          int
	firstDelivered chan struct{}
	finished       chan struct{}
}

func (*appendPhaseChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, errors.New("unexpected non-stream model call")
}
func (*appendPhaseChat) GetModelName() string { return "append-phase-test" }
func (*appendPhaseChat) GetModelID() string   { return "append-phase-test" }
func (m *appendPhaseChat) ChatStream(ctx context.Context, _ []chat.Message, opts *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.calls++
	out := make(chan types.StreamResponse, 1)
	if m.calls == 1 {
		out <- types.StreamResponse{Content: "先查询资料。", ToolCalls: []types.LLMToolCall{{ID: "lookup", Type: "function", Function: types.FunctionCall{Name: "lookup", Arguments: `{}`}}}, Done: true, FinishReason: "tool_calls"}
		close(out)
		return out, nil
	}
	if m.calls != 2 || opts.ToolChoice == "none" || len(opts.Tools) == 0 {
		return nil, errors.New("unexpected model phase")
	}
	go func() {
		defer close(out)
		select {
		case <-m.firstDelivered:
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			out <- types.StreamResponse{ResponseType: types.ResponseTypeError, Content: "phase update was buffered", Done: true}
			return
		}
		out <- types.StreamResponse{Content: "第一段🙂\n\n第二段", Done: true, FinishReason: "stop"}
		close(m.finished)
	}()
	return out, nil
}

type appendLookupTool struct{ agenttools.BaseTool }

func (*appendLookupTool) Execute(context.Context, json.RawMessage) (*types.ToolResult, error) {
	return &types.ToolResult{Success: true, Output: "found"}, nil
}

func TestWechatAppendRealAgentSendsPhaseWithoutExtraModelCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model := &appendPhaseChat{firstDelivered: make(chan struct{}), finished: make(chan struct{})}
		var sent []string
		beforeCompletion := false
		service := &Service{messageService: &deliveryMessageService{}, streamManager: &fullOutputStreamManager{}, sessionService: &exitSessionService{run: func(ctx context.Context, bus *event.EventBus) error {
			registry := agenttools.NewToolRegistry()
			registry.RegisterTool(&appendLookupTool{BaseTool: agenttools.NewBaseTool("lookup", "lookup", json.RawMessage(`{"type":"object"}`))})
			engine := agent.NewAgentEngine(&types.AgentConfig{MaxIterations: 3}, model, registry, bus, nil, nil, "session", "")
			_, err := engine.Execute(ctx, "session", "message", "question", nil)
			return err
		}}}
		writer := newAppendReply(&IMChannel{}, func(_ context.Context, text string) error {
			sent = append(sent, text)
			if text == "先查询资料。" {
				select {
				case <-model.finished:
				default:
					beforeCompletion = true
				}
				close(model.firstDelivered)
			}
			return nil
		})
		err := service.handleMessageOutput(context.Background(), &IncomingMessage{Platform: PlatformWeChat, UserID: "user", Content: "question"}, &types.Session{ID: "session"}, streamExitAgent(), nil, nil, nil, nil, nil, "user", nil, writer)
		require.NoError(t, err)
		require.True(t, beforeCompletion, "confirmed phase must arrive before the final model round completes")
		require.Equal(t, 2, model.calls)
		require.Equal(t, []string{"先查询资料。", "第一段🙂\n\n第二段"}, sent)
		require.Equal(t, "第一段🙂\n\n第二段", service.messageService.(*deliveryMessageService).saved.Content)
		require.Equal(t, sent[1], service.messageService.(*deliveryMessageService).saved.Content)
	})
}

func TestWechatAppendProgressReplayAndPartialFailure(t *testing.T) {
	var sent []string
	attempts := 0
	writer := newAppendReply(&IMChannel{}, func(_ context.Context, text string) error {
		attempts++
		if attempts == 2 {
			return errors.New("unknown delivery")
		}
		sent = append(sent, text)
		return nil
	})
	parts := IMStreamParts{Progress: []IMProgressStep{{ID: "phase", Content: strings.Repeat("🙂", 2001)}}}
	require.ErrorContains(t, writer.flush(context.Background(), parts, false), "unknown delivery")
	require.Equal(t, strings.Repeat("🙂", 2000), writer.progress["phase"])
	require.Error(t, writer.flush(context.Background(), parts, true))
	require.Equal(t, 2, attempts)
	require.Len(t, sent, 1)
	var replayed []string
	writer = newAppendReply(&IMChannel{}, func(_ context.Context, text string) error { replayed = append(replayed, text); return nil })
	parts = IMStreamParts{Progress: []IMProgressStep{{ID: "phase", Content: "阶段完成"}}, Answer: "最终正文"}
	require.NoError(t, writer.flush(context.Background(), parts, false))
	require.NoError(t, writer.flush(context.Background(), parts, true))
	require.Equal(t, []string{"阶段完成", "最终正文"}, replayed)
}
