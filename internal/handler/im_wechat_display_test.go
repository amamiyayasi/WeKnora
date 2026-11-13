package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/im"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestWechatDisplayAPIPreservesSettingsAndCredentials(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "im.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.Exec(`CREATE TABLE im_channels (
id TEXT PRIMARY KEY,tenant_id INTEGER,agent_id TEXT,platform TEXT,name TEXT,enabled BOOLEAN,
mode TEXT,output_mode TEXT,locale TEXT,session_mode TEXT,knowledge_base_id TEXT,credentials TEXT,
bot_identity TEXT,show_thinking BOOLEAN DEFAULT FALSE,show_tool_hints BOOLEAN DEFAULT FALSE,
created_at DATETIME,updated_at DATETIME,deleted_at DATETIME)`).Error)
	agents := &savingAgentService{stored: &types.CustomAgent{ID: "agent", TenantID: 7}}
	svc := im.NewService(db, nil, nil, nil, agents, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	t.Cleanup(svc.Stop)
	h := NewIMHandler(svc)
	r := gin.New()
	r.POST("/agents/:id/im-channels", h.CreateIMChannel)
	r.PUT("/im-channels/:id", h.UpdateIMChannel)
	r.GET("/agents/:id/im-channels", h.ListIMChannels)
	request := func(method, path, body string, tenant uint64) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(context.WithValue(req.Context(), types.TenantIDContextKey, tenant))
		res := httptest.NewRecorder()
		r.ServeHTTP(res, req)
		return res
	}
	created := request(http.MethodPost, "/agents/agent/im-channels", `{"platform":"wechat","credentials":{"bot_token":"private-bot","ilink_bot_id":"bot"},"enabled":false}`, 7)
	require.Equal(t, 200, created.Code, created.Body.String())
	var response struct {
		Data im.IMChannel `json:"data"`
	}
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &response))
	id := response.Data.ID
	require.Equal(t, "stream", response.Data.OutputMode)
	require.Equal(t, "longpoll", response.Data.Mode)
	changed := request(http.MethodPut, "/im-channels/"+id, `{"show_thinking":true,"show_tool_hints":true,"enabled":false}`, 7)
	require.Equal(t, 200, changed.Code, changed.Body.String())
	changed = request(http.MethodPut, "/im-channels/"+id, `{"output_mode":"full","mode":"webhook"}`, 7)
	require.Equal(t, 200, changed.Code, changed.Body.String())
	stored, err := svc.GetChannelByIDAndTenant(id, 7)
	require.NoError(t, err)
	require.Equal(t, "full", stored.OutputMode)
	require.Equal(t, "longpoll", stored.Mode)
	require.True(t, stored.ShowThinking)
	require.True(t, stored.ShowToolHints)
	require.Contains(t, string(stored.Credentials), "private-bot")
	listed := request(http.MethodGet, "/agents/agent/im-channels", "", 7)
	require.Equal(t, 200, listed.Code)
	require.Contains(t, listed.Body.String(), `"show_thinking":true`)
	require.NotContains(t, listed.Body.String(), "private-bot")
	require.Equal(t, 404, request(http.MethodPut, "/im-channels/"+id, `{"show_thinking":false}`, 8).Code)
	require.Equal(t, 400, request(http.MethodPut, "/im-channels/"+id, `{"output_mode":"invalid"}`, 7).Code)
}
