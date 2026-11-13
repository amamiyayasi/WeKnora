package im

import (
	"context"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
)

// AppendSender sends new text rather than replacing a platform card.
type AppendSender interface {
	SendChunk(context.Context, *IncomingMessage, string) error
}

// ReplyLifecycle is optional; the returned cleanup also covers failure/cancel.
type ReplyLifecycle interface {
	BeginReply(context.Context, *IncomingMessage) func()
}

const appendTextLimit = 2000

// SplitReplyText keeps Unicode characters and whitespace intact across messages.
func SplitReplyText(text string) []string {
	var parts []string
	for text != "" {
		part := nextAppendText(text, true)
		parts = append(parts, part)
		text = text[len(part):]
	}
	return parts
}

func nextAppendText(text string, final bool) string {
	runes := []rune(text)
	if len(runes) > appendTextLimit {
		runes = runes[:appendTextLimit]
	}
	window := string(runes)
	if !final || len(window) < len(text) {
		if cut := strings.LastIndex(window, "\n\n"); cut >= 0 {
			return window[:cut+2]
		}
		if len([]rune(text)) >= appendTextLimit {
			if cut := strings.LastIndex(window, "\n"); cut >= 0 {
				return window[:cut+1]
			}
			return window
		}
		if !final {
			return ""
		}
	}
	return window
}

type appendReply struct {
	clean                   func(context.Context, string) string
	showThinking, showTools bool
	send                    func(context.Context, string) error
	answer, thought         string
	revision                int
	tools                   map[string]string
	err                     error
}

func newAppendReply(channel *IMChannel, send func(context.Context, string) error) *appendReply {
	return &appendReply{showThinking: channel.ShowThinking, showTools: channel.ShowToolHints, send: send, tools: map[string]string{}}
}

func (a *appendReply) deliver(ctx context.Context, text string) error {
	if a.err != nil {
		return a.err
	}
	if err := ctx.Err(); err != nil {
		a.err = err
		return err
	}
	if a.clean != nil {
		text = a.clean(ctx, text)
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}
	a.err = a.send(ctx, text)
	if a.err == nil {
		logger.Infof(ctx, "[IM] Append chunk sent: runes=%d", len([]rune(text)))
	}
	return a.err
}

func (a *appendReply) text(ctx context.Context, full string, sent *string, prefix string, final bool) error {
	if strings.HasPrefix(*sent, full) && strings.TrimSpace(*sent) == strings.TrimSpace(full) {
		return nil
	}
	if !strings.HasPrefix(full, *sent) {
		// Already-visible text cannot be retracted. Never replay a rewritten answer.
		a.err = errors.New("append reply changed after delivery")
		return a.err
	}
	for rest := full[len(*sent):]; rest != ""; rest = full[len(*sent):] {
		part := nextAppendText(rest, final)
		if part == "" {
			break
		}
		// Prefixes count toward the platform's message length cap.
		if prefix != "" && len([]rune(prefix+part)) > appendTextLimit {
			part = string([]rune(part)[:appendTextLimit-len([]rune(prefix))])
		}
		if err := a.deliver(ctx, prefix+part); err != nil {
			return err
		}
		*sent += part
		if !final {
			return nil
		}
	}
	return nil
}

func (a *appendReply) flush(ctx context.Context, parts IMStreamParts, final bool) error {
	if a.err != nil {
		return a.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if parts.AnswerRevision != a.revision {
		if a.answer != "" {
			if err := a.deliver(ctx, "回答已修订，请以下面的内容为准。"); err != nil {
				return err
			}
		}
		a.answer = ""
		a.revision = parts.AnswerRevision
	}
	if a.showThinking {
		if err := a.text(ctx, parts.ExplicitThought, &a.thought, "思考：", final); err != nil {
			return err
		}
	}
	if a.showTools {
		for _, step := range append(append([]IMToolStep(nil), parts.PipelineToolSteps...), parts.AgentToolSteps...) {
			if !isToolVisibleToUser(step.ToolName) {
				continue
			}
			status := "开始"
			if !step.Pending {
				if step.Success {
					status = "完成"
				} else {
					status = "失败"
				}
			}
			if a.tools[step.ToolCallID] == status {
				continue
			}
			label := imLocalizedToolName(step.ToolName) + "：" + status
			for _, text := range SplitReplyText(label) {
				if err := a.deliver(ctx, text); err != nil {
					return err
				}
			}
			a.tools[step.ToolCallID] = status
		}
	}
	return a.text(ctx, parts.Answer, &a.answer, "", final)
}

// Failure discards queued progress. A cancelled send may be followed by one
// distinct error notice, but an unknown network outcome is never retried.
func (a *appendReply) fail(ctx context.Context, err error) error {
	if a.err != nil && !errors.Is(a.err, context.Canceled) && !errors.Is(a.err, context.DeadlineExceeded) {
		return a.err
	}
	a.err = nil
	return a.deliver(ctx, imQAFailureReply(err))
}
