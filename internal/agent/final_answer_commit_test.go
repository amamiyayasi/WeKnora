package agent

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestFinalSynthesisMarksChunksForAppendDelivery(t *testing.T) {
	e := newTestEngine(t, &mockChat{responses: []mockResponse{{chunks: []types.StreamResponse{
		{ResponseType: types.ResponseTypeAnswer, Content: "第一段\n\n"},
		{ResponseType: types.ResponseTypeAnswer, Content: "第二段", Done: true, FinishReason: "stop"},
	}}}})
	var text string
	var done bool
	e.eventBus.On(event.EventAgentFinalAnswer, func(_ context.Context, evt event.Event) error {
		data := evt.Data.(event.AgentFinalAnswerData)
		require.True(t, data.Committed, "tool-free final synthesis must be distinguishable from ReAct drafts")
		text += data.Content
		done = data.Done
		return nil
	})
	state := &types.AgentState{}
	require.NoError(t, e.streamFinalAnswerToEventBus(context.Background(), "question", state, "session", nil))
	require.True(t, done)
	require.Equal(t, "第一段\n\n第二段", text)
	require.Equal(t, text, state.FinalAnswer)
}
