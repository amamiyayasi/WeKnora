package database

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWechatDisplayMigrationPreservesOldModesAndDefaults(t *testing.T) {
	for _, migration := range []string{"sqlite/000053_wechat_display"} {
		t.Run(migration, func(t *testing.T) {
			db := openSQLiteDB(t, filepath.Join(t.TempDir(), "channels.db"))
			_, err := db.Exec("CREATE TABLE im_channels (id TEXT,output_mode TEXT)")
			require.NoError(t, err)
			_, err = db.Exec("INSERT INTO im_channels VALUES ('wechat','full'),('other','stream')")
			require.NoError(t, err)
			up, err := os.ReadFile(filepath.Join(sqliteRepoRoot(t), "migrations", migration+".up.sql"))
			require.NoError(t, err)
			_, err = db.Exec(string(up))
			require.NoError(t, err)
			var mode string
			var thinking, tools bool
			require.NoError(t, db.QueryRow("SELECT output_mode,show_thinking,show_tool_hints FROM im_channels WHERE id='wechat'").Scan(&mode, &thinking, &tools))
			require.Equal(t, "full", mode)
			require.False(t, thinking)
			require.False(t, tools)
			_, err = db.Exec("UPDATE im_channels SET show_thinking=TRUE,show_tool_hints=TRUE WHERE id='wechat'")
			require.NoError(t, err)
			require.NoError(t, db.QueryRow("SELECT show_thinking,show_tool_hints FROM im_channels WHERE id='wechat'").Scan(&thinking, &tools))
			require.True(t, thinking)
			require.True(t, tools)
			down, err := os.ReadFile(filepath.Join(sqliteRepoRoot(t), "migrations", migration+".down.sql"))
			require.NoError(t, err)
			_, err = db.Exec(string(down))
			require.NoError(t, err)
			require.NoError(t, db.QueryRow("SELECT output_mode FROM im_channels WHERE id='wechat'").Scan(&mode))
			require.Equal(t, "full", mode)
		})
	}
}
