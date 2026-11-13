package wechat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tencent/WeKnora/internal/im"
)

type ilinkTransport func(*http.Request) (*http.Response, error)

func (f ilinkTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTypingFetchesTicketAndChecksBusinessErrors(t *testing.T) {
	for _, tc := range []struct {
		name, config, typing string
		wantErr              bool
	}{
		{"success", `{"ret":0,"typing_ticket":"ticket"}`, `{"ret":0}`, false},
		{"missing ticket", `{"ret":0}`, `{"ret":0}`, true},
		{"config error", `{"ret":-1,"errmsg":"secret"}`, `{"ret":0}`, true},
		{"typing error", `{"ret":0,"typing_ticket":"ticket"}`, `{"errcode":-2,"errmsg":"secret"}`, true},
		{"invalid success body", `{"ret":0,"typing_ticket":"ticket"}`, `null`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := ilinkHTTPClient
			t.Cleanup(func() { ilinkHTTPClient = old })
			var paths []string
			ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
				paths = append(paths, r.URL.Path)
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["ilink_user_id"] != "user" {
					t.Fatalf("wrong recipient: %v", body)
				}
				response := tc.config
				if strings.HasSuffix(r.URL.Path, "getconfig") {
					if body["context_token"] != "context" {
						t.Fatalf("missing context: %v", body)
					}
				} else {
					if body["typing_ticket"] != "ticket" || body["status"] != float64(1) {
						t.Fatalf("wrong typing: %v", body)
					}
					response = tc.typing
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
			})}
			err := NewAdapter("bot", "id").SendTyping(context.Background(), &im.IncomingMessage{UserID: "user", Extra: map[string]string{"context_token": "context"}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v paths=%v", err, tc.wantErr, paths)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("upstream response leaked")
			}
			if tc.name == "success" && len(paths) != 2 {
				t.Fatalf("calls=%v", paths)
			}
		})
	}
}

func TestTypingRemainsActiveAcrossPhaseAndFinalMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		var steps []string
		var sentAt []time.Time
		var mu sync.Mutex
		snapshot := func() string { mu.Lock(); defer mu.Unlock(); return strings.Join(steps, ",") }
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			defer mu.Unlock()
			response := `{"ret":0}`
			switch r.URL.Path {
			case "/ilink/bot/getconfig":
				response = `{"ret":0,"typing_ticket":"ticket"}`
			case "/ilink/bot/sendtyping":
				if body["status"] == float64(1) {
					steps = append(steps, "start")
				} else {
					steps = append(steps, "stop")
				}
			case "/ilink/bot/sendmessage":
				steps = append(steps, "text")
				sentAt = append(sentAt, time.Now())
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
		})}
		adapter := NewAdapter("bot", "id")
		msg := &im.IncomingMessage{MessageID: "analysis", UserID: "u"}
		stop := adapter.BeginReply(context.Background(), msg)
		synctest.Wait()
		if err := adapter.SendChunk(context.Background(), msg, "阶段正文"); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if snapshot() != "start,text,start" {
			t.Fatalf("phase must reassert typing without stop: %s", snapshot())
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if snapshot() != "start,text,start,start" {
			t.Fatalf("renewal stopped after phase: %s", snapshot())
		}
		if err := adapter.SendReply(context.Background(), msg, &im.ReplyMessage{Content: "最终正文"}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		stop()
		if snapshot() != "start,text,start,start,text,start,stop" {
			t.Fatalf("turn did not stop after final delivery: %s", snapshot())
		}
		if err := adapter.SendChunk(context.Background(), &im.IncomingMessage{MessageID: "next", UserID: "u"}, "独立回复"); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		mu.Lock()
		gap := sentAt[2].Sub(sentAt[1])
		mu.Unlock()
		if gap < 2*time.Second {
			t.Fatal("messages sent too quickly")
		}
		before := snapshot()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if snapshot() != before {
			t.Fatalf("typing restarted after completion: %s", snapshot())
		}
	})
}

func TestTypingTimeoutDoesNotBlockAnswerOrRestartAfterStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		var typing int
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "getconfig") {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			if strings.HasSuffix(r.URL.Path, "sendtyping") {
				typing++
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ret":0}`))}, nil
		})}
		adapter := NewAdapter("bot", "id")
		msg := &im.IncomingMessage{UserID: "u"}
		stop := adapter.BeginReply(context.Background(), msg)
		synctest.Wait()
		started := time.Now()
		if err := adapter.SendReply(context.Background(), msg, &im.ReplyMessage{Content: "回答"}); err != nil {
			t.Fatal(err)
		}
		stop()
		if time.Since(started) > 3*time.Second {
			t.Fatal("typing blocked answer")
		}
		if typing != 0 {
			t.Fatal("late typing started after stopping")
		}
	})
}

func TestTypingAndPacingAreScopedToChannelAndUser(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		var texts int
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "sendmessage") {
				texts++
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ret":0,"typing_ticket":"ticket"}`))}, nil
		})}
		a, b := NewAdapter("a", "a"), NewAdapter("b", "b")
		ctx, cancel := context.WithCancel(context.Background())
		a.channelCtx = ctx
		stopA := a.BeginReply(ctx, &im.IncomingMessage{UserID: "same"})
		stopB := b.BeginReply(context.Background(), &im.IncomingMessage{UserID: "same"})
		synctest.Wait()
		cancel()
		stopA()
		if _, ok := b.turns.Load("same"); !ok {
			t.Fatal("another channel's light stopped")
		}
		stopB()
		start := time.Now()
		for _, msg := range []*im.IncomingMessage{{UserID: "one"}, {UserID: "two"}} {
			if err := b.SendChunk(context.Background(), msg, "hi"); err != nil {
				t.Fatal(err)
			}
		}
		if texts != 2 || time.Since(start) != 0 {
			t.Fatal("unrelated users share pacing")
		}
		if err := a.SendChunk(context.Background(), &im.IncomingMessage{UserID: "same"}, "late"); err == nil {
			t.Fatal("disabled channel sent content")
		}
	})
}

func TestSendFailurePreservesSafeReasonAndAcceptedProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		attempts := 0
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			attempts++
			response := `{"ret":0}`
			if attempts >= 2 {
				response = `{"ret":-2,"errmsg":"prepare failed"}`
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
		})}
		adapter := NewAdapter("bot-secret", "id")
		msg := &im.IncomingMessage{MessageID: "request", UserID: "user", Extra: map[string]string{"context_token": "ticket-secret"}}
		err := adapter.SendChunk(context.Background(), msg, strings.Repeat("中", 4001))
		if err == nil || !strings.Contains(err.Error(), "accepted_parts_in_send=1") || !strings.Contains(err.Error(), "reason=prepare_failed") {
			t.Fatalf("missing delivery diagnostic: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("unexpected retry: %d", attempts)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "中") {
			t.Fatal("sensitive send metadata leaked")
		}
		msg.MessageID = "next-request"
		err = adapter.SendChunk(context.Background(), msg, "next message")
		if err == nil || !strings.Contains(err.Error(), "accepted_parts_in_send=0") {
			t.Fatalf("progress leaked across requests: %v", err)
		}
	})
}

func TestIlinkDoesNotExposeUnknownErrorText(t *testing.T) {
	old := ilinkHTTPClient
	defer func() { ilinkHTTPClient = old }()
	ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ret":-2,"errmsg":"credential-or-body-secret"}`))}, nil
	})}
	err := NewAdapter("bot", "id").SendChunk(context.Background(), &im.IncomingMessage{MessageID: "request", UserID: "user"}, "private body")
	if err == nil || !strings.Contains(err.Error(), "reason=other") || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private body") {
		t.Fatalf("unsafe diagnostic: %v", err)
	}
}

func TestTypingCancelWaitsForPostMessageStartAndDoesNotRelight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		starts, stops := 0, 0
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			response := `{"ret":0,"typing_ticket":"ticket"}`
			if strings.HasSuffix(r.URL.Path, "sendtyping") {
				if body["status"] == float64(1) {
					starts++
					if starts == 2 {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
				} else {
					stops++
				}
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
		})}
		adapter := NewAdapter("bot", "id")
		msg := &im.IncomingMessage{MessageID: "analysis", UserID: "u"}
		stop := adapter.BeginReply(context.Background(), msg)
		synctest.Wait()
		if err := adapter.SendChunk(context.Background(), msg, "阶段正文"); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if starts != 2 {
			t.Fatalf("missing post-message renewal: %d", starts)
		}
		stop()
		synctest.Wait()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if starts != 2 || stops != 1 {
			t.Fatalf("cancel race: starts=%d stops=%d", starts, stops)
		}
	})
}

func TestTypingSurvivesCommandButStopsOnOwnDeliveryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		stops := 0
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			response := `{"ret":0,"typing_ticket":"ticket"}`
			if strings.HasSuffix(r.URL.Path, "sendmessage") {
				response = `{"ret":-2}`
			} else if strings.HasSuffix(r.URL.Path, "sendtyping") && body["status"] == float64(2) {
				stops++
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response))}, nil
		})}
		adapter := NewAdapter("bot", "id")
		msg := &im.IncomingMessage{MessageID: "analysis", UserID: "u"}
		stop := adapter.BeginReply(context.Background(), msg)
		synctest.Wait()
		if err := adapter.SendChunk(context.Background(), &im.IncomingMessage{MessageID: "command", UserID: "u"}, "指令回复"); err == nil {
			t.Fatal("expected rejection")
		}
		synctest.Wait()
		if stops != 0 {
			t.Fatal("command reply stopped active analysis")
		}
		if err := adapter.SendChunk(context.Background(), msg, "阶段正文"); err == nil {
			t.Fatal("expected rejection")
		}
		synctest.Wait()
		if stops != 1 {
			t.Fatalf("failed delivery did not clear typing: %d", stops)
		}
		stop()
	})
}

func TestTypingCancellationClearsUnknownFirstStartOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		old := ilinkHTTPClient
		defer func() { ilinkHTTPClient = old }()
		starts, stops := 0, 0
		ilinkHTTPClient = &http.Client{Transport: ilinkTransport(func(r *http.Request) (*http.Response, error) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if strings.HasSuffix(r.URL.Path, "sendtyping") {
				if body["status"] == float64(1) {
					starts++
					<-r.Context().Done()
					return nil, r.Context().Err()
				}
				stops++
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ret":0,"typing_ticket":"ticket"}`))}, nil
		})}
		adapter := NewAdapter("bot", "id")
		stop := adapter.BeginReply(context.Background(), &im.IncomingMessage{UserID: "u"})
		synctest.Wait()
		stop()
		if starts != 1 || stops != 1 {
			t.Fatalf("unknown initial status must be cleared: starts=%d stops=%d", starts, stops)
		}
	})
}
