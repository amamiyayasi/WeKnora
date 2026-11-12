package im

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendReplySendsOnlyNewConfirmedText(t *testing.T) {
	var sent []string
	reply := newAppendReply(&IMChannel{}, func(_ context.Context, text string) error { sent = append(sent, text); return nil })
	ctx := context.Background()
	require.NoError(t, reply.flush(ctx, IMStreamParts{LiveAnswer: "草稿不能发送"}, false))
	require.Empty(t, sent)
	require.NoError(t, reply.flush(ctx, IMStreamParts{Answer: "第一段🙂\n\n第二段"}, false))
	require.Equal(t, []string{"第一段🙂\n\n"}, sent)
	require.NoError(t, reply.flush(ctx, IMStreamParts{Answer: "第一段🙂\n\n第二段"}, true))
	require.Equal(t, []string{"第一段🙂\n\n", "第二段"}, sent)
	require.NoError(t, reply.flush(ctx, IMStreamParts{Answer: "第一段🙂\n\n第二段"}, true))
	require.Len(t, sent, 2)
}

func TestAppendReplyCleansNewChunksWithoutResigningSentPrefix(t *testing.T) {
	var sent []string
	reply := newAppendReply(&IMChannel{}, func(_ context.Context, s string) error { sent = append(sent, s); return nil })
	calls := 0
	reply.clean = func(_ context.Context, text string) string {
		calls++
		return strings.ReplaceAll(text, "local://source.png", fmt.Sprintf("https://files/%d", calls))
	}
	parts := IMStreamParts{Answer: "附件 local://source.png\n\n后续正文"}
	require.NoError(t, reply.flush(context.Background(), parts, false))
	require.NoError(t, reply.flush(context.Background(), parts, true))
	require.Equal(t, []string{"附件 https://files/1\n\n", "后续正文"}, sent)
}

func TestAppendReplyFiltersProgressAndStopsAfterUnknownSend(t *testing.T) {
	for _, show := range []bool{false, true} {
		var sent []string
		reply := newAppendReply(&IMChannel{ShowThinking: show, ShowToolHints: show}, func(_ context.Context, s string) error { sent = append(sent, s); return nil })
		parts := IMStreamParts{ExplicitThought: "正在核实来源\n\n", AgentInner: "被替代的草稿", AgentToolSteps: []IMToolStep{{ToolCallID: "a", ToolName: "web_search", Pending: true, Arguments: map[string]any{"token": "secret"}, Output: "private"}}}
		require.NoError(t, reply.flush(context.Background(), parts, false))
		if show {
			require.Equal(t, []string{"思考：正在核实来源\n\n", "网络搜索：开始"}, sent)
		} else {
			require.Empty(t, sent)
		}
	}
	var attempts int
	reply := newAppendReply(&IMChannel{}, func(context.Context, string) error { attempts++; return errors.New("unknown send") })
	require.Error(t, reply.flush(context.Background(), IMStreamParts{Answer: "回答"}, true))
	require.Error(t, reply.flush(context.Background(), IMStreamParts{Answer: "回答"}, true))
	require.Equal(t, 1, attempts)
}

func TestSplitReplyTextPreservesUnicodeAndWhitespace(t *testing.T) {
	input := strings.Repeat("中🙂", 2100) + "\n\n尾段"
	parts := SplitReplyText(input)
	require.Equal(t, input, strings.Join(parts, ""))
	for _, part := range parts {
		require.LessOrEqual(t, len([]rune(part)), 2000)
	}
}

func TestAppendReplyCancellationDiscardsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var attempts int
	reply := newAppendReply(&IMChannel{ShowThinking: true}, func(context.Context, string) error { attempts++; return nil })
	require.ErrorIs(t, reply.flush(ctx, IMStreamParts{Answer: "正文", ExplicitThought: "思考"}, true), context.Canceled)
	require.Zero(t, attempts)
}

func TestAppendReplyProviderReplacementLabelsAlreadyVisibleRevision(t *testing.T) {
	var sent []string
	reply := newAppendReply(&IMChannel{}, func(_ context.Context, s string) error { sent = append(sent, s); return nil })
	require.NoError(t, reply.flush(context.Background(), IMStreamParts{Answer: "初稿\n\n尚未发送"}, false))
	require.NoError(t, reply.flush(context.Background(), IMStreamParts{Answer: "修订后的完整回答", AnswerRevision: 1}, true))
	require.Equal(t, []string{"初稿\n\n", "回答已修订，请以下面的内容为准。", "修订后的完整回答"}, sent)
}
