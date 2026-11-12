package wechat

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/im"
	"github.com/Tencent/WeKnora/internal/logger"
)

type typingTurn struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func (t *typingTurn) stop() { t.once.Do(t.cancel); <-t.done }

// BeginReply owns one turn's typing light; visible content stops it first.
func (a *Adapter) BeginReply(ctx context.Context, msg *im.IncomingMessage) func() {
	ctx, cancel := context.WithCancel(ctx)
	turn := &typingTurn{cancel: cancel, done: make(chan struct{})}
	if old, loaded := a.turns.Swap(msg.UserID, turn); loaded {
		old.(*typingTurn).stop()
	}
	stopChannel := func() bool { return false }
	if a.channelCtx != nil {
		stopChannel = context.AfterFunc(a.channelCtx, cancel)
	}
	go func() {
		defer close(turn.done)
		defer stopChannel()
		defer a.turns.CompareAndDelete(msg.UserID, turn)
		started, warned := false, false
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			if ctx.Err() == nil {
				err := a.setTyping(ctx, msg, 1)
				if err == nil {
					started = true
				} else if !warned {
					logger.Warn(ctx, "[WeChat] Typing indicator unavailable")
					warned = true
				}
			}
			select {
			case <-ctx.Done():
				if started {
					_ = a.setTyping(context.WithoutCancel(ctx), msg, 2)
				}
				return
			case <-ticker.C:
			}
		}
	}()
	return turn.stop
}

func (a *Adapter) SendTyping(ctx context.Context, msg *im.IncomingMessage) error {
	return a.setTyping(ctx, msg, 1)
}

func (a *Adapter) setTyping(ctx context.Context, msg *im.IncomingMessage, status int) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	var config struct {
		Ticket string `json:"typing_ticket"`
	}
	if err := a.ilinkJSON(ctx, "/ilink/bot/getconfig", map[string]any{
		"ilink_user_id": msg.UserID, "context_token": msg.Extra["context_token"], "base_info": newBaseInfo(),
	}, &config); err != nil {
		return err
	}
	if config.Ticket == "" {
		return errors.New("missing typing ticket")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.ilinkPost(ctx, "/ilink/bot/sendtyping", map[string]any{
		"ilink_user_id": msg.UserID, "typing_ticket": config.Ticket, "status": status, "base_info": newBaseInfo(),
	})
}
