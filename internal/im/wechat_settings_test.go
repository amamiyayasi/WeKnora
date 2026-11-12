package im

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWechatDisplaySettingsPersistAndInvalidateRuntime(t *testing.T) {
	channel := IMChannel{Platform: "wechat", OutputMode: "full"}
	require.NoError(t, json.Unmarshal([]byte(`{"show_thinking":true,"show_tool_hints":true}`), &channel))
	data, err := json.Marshal(SummarizeIMChannel(channel))
	require.NoError(t, err)
	var summary map[string]any
	require.NoError(t, json.Unmarshal(data, &summary))
	require.Equal(t, true, summary["show_thinking"])
	require.Equal(t, true, summary["show_tool_hints"])
	fresh := IMChannel{Platform: "wechat", OutputMode: "full"}
	require.False(t, sameChannelRuntimeConfig(&channel, &fresh))
}
