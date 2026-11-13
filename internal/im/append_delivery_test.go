package im

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tencent/WeKnora/internal/event"
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
