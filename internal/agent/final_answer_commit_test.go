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

func TestAppendOnlyNaturalStopEntersCommittedFinalStage(t *testing.T) {
	model := &mockChat{responses: []mockResponse{
		{chunks: []types.StreamResponse{{Content: "ready", Done: true, FinishReason: "stop"}}},
		{chunks: []types.StreamResponse{{Content: "第一段\n\n"}, {Content: "第二段", Done: true, FinishReason: "stop"}}},
	}}
	e := newTestEngine(t, model)
	ctx := context.WithValue(context.Background(), types.AppendOnlyAnswerContextKey, true)
	complete := false
	var visible string
	e.eventBus.On(event.EventAgentComplete, func(context.Context, event.Event) error { complete = true; return nil })
	e.eventBus.On(event.EventAgentFinalAnswer, func(_ context.Context, evt event.Event) error {
		d := evt.Data.(event.AgentFinalAnswerData)
		if d.Committed && d.Content != "" {
			require.False(t, complete)
			visible += d.Content
		}
		return nil
	})
	state := &types.AgentState{}
	_, err := e.executeLoop(ctx, state, "question", emptyMessages(), nil, "session", "message")
	require.NoError(t, err)
	require.Equal(t, "第一段\n\n第二段", state.FinalAnswer)
	require.Equal(t, state.FinalAnswer, visible)
	require.Equal(t, 2, model.callCount)
	require.Equal(t, "none", model.opts[1].ToolChoice)
	require.Empty(t, model.opts[1].Tools)
}

func TestReplaceableNaturalStopKeepsSingleCall(t *testing.T) {
	model := &mockChat{responses: []mockResponse{{chunks: []types.StreamResponse{{Content: "直接回答", Done: true, FinishReason: "stop"}}}}}
	e := newTestEngine(t, model)
	state := &types.AgentState{}
	_, err := e.executeLoop(context.Background(), state, "question", emptyMessages(), nil, "session", "message")
	require.NoError(t, err)
	require.Equal(t, "直接回答", state.FinalAnswer)
	require.Equal(t, 1, model.callCount)
}

func TestAppendOnlyDoesNotRegenerateFilteredOrTruncatedAnswer(t *testing.T) {
	for _, finish := range []string{"content_filter", "length"} {
		t.Run(finish, func(t *testing.T) {
			model := &mockChat{responses: []mockResponse{{chunks: []types.StreamResponse{{Content: "终止回答", Done: true, FinishReason: finish}}}}}
			e := newTestEngine(t, model)
			ctx := context.WithValue(context.Background(), types.AppendOnlyAnswerContextKey, true)
			state := &types.AgentState{}
			_, err := e.executeLoop(ctx, state, "question", emptyMessages(), nil, "session", "message")
			require.NoError(t, err)
			require.Equal(t, 1, model.callCount)
		})
	}
}
