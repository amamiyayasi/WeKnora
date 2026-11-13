// Adapter implements im.Adapter and im.FileDownloader for WeChat personal
// accounts via the Tencent iLink Bot API.
//
// WeChat iLink uses HTTP long-polling for receiving messages (no WebSocket,
// no Webhook). Sending is done via REST API.
//
// API base: https://ilinkai.weixin.qq.com
// API paths: /ilink/bot/getupdates, /ilink/bot/sendmessage, etc.
// Auth: Bearer token obtained via QR code login flow.
package wechat

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/im"
	"github.com/Tencent/WeKnora/internal/logger"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
)

const (
	ilinkBaseURL = "https://ilinkai.weixin.qq.com"
	// cdnBaseURL is the Weixin CDN base for media download/upload.
	cdnBaseURL = "https://novac2c.cdn.weixin.qq.com/c2c"
	// defaultBotType is the bot_type for iLink get_bot_qrcode / get_qrcode_status.
	defaultBotType = "3"
	// channelVersion is sent in base_info with every API request.
	channelVersion = "weknora-1.0.0"
)

var ilinkHTTPClient = secutils.NewSSRFSafeHTTPClient(secutils.SSRFSafeHTTPClientConfig{
	Timeout:      30 * time.Second,
	MaxRedirects: 5,
})

// BuildCDNDownloadURL constructs a CDN download URL from an encrypt_query_param.
func BuildCDNDownloadURL(encryptQueryParam string) string {
	return cdnBaseURL + "/download?encrypted_query_param=" + url.QueryEscape(encryptQueryParam)
}

// Compile-time interface checks.
var (
	_ im.Adapter        = (*Adapter)(nil)
	_ im.FileDownloader = (*Adapter)(nil)
)

// baseInfo is included in every outgoing API request body.
type baseInfo struct {
	ChannelVersion string `json:"channel_version"`
}

func newBaseInfo() baseInfo {
	return baseInfo{ChannelVersion: channelVersion}
}

// Adapter implements im.Adapter for WeChat via iLink Bot API.
type Adapter struct {
	channelCtx context.Context
	turns      sync.Map
	deliveries sync.Map
	botToken   string
	ilinkBotID string
}

// NewAdapter creates a new WeChat iLink adapter.
func NewAdapter(botToken, ilinkBotID string) *Adapter {
	return &Adapter{
		botToken:   botToken,
		ilinkBotID: ilinkBotID,
	}
}

func (a *Adapter) Platform() im.Platform {
	return im.PlatformWeChat
}

// VerifyCallback is not supported — WeChat iLink uses long-polling, not webhooks.
func (a *Adapter) VerifyCallback(c *gin.Context) error {
	return fmt.Errorf("WeChat adapter does not support webhook callbacks")
}

// ParseCallback is not supported — messages arrive via long-polling.
func (a *Adapter) ParseCallback(c *gin.Context) (*im.IncomingMessage, error) {
	return nil, fmt.Errorf("WeChat adapter does not support webhook callbacks")
}

// HandleURLVerification is not applicable for WeChat.
func (a *Adapter) HandleURLVerification(c *gin.Context) bool {
	return false
}

type userDelivery struct {
	mu            sync.Mutex
	last          time.Time
	messageID     string
	acceptedParts int
}

// SendChunk serializes messages for this adapter's user, without retrying unknown sends.
func (a *Adapter) SendChunk(ctx context.Context, incoming *im.IncomingMessage, text string) error {
	if a.channelCtx != nil {
		if err := a.channelCtx.Err(); err != nil {
			return err
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		stop := context.AfterFunc(a.channelCtx, cancel)
		defer stop()
		defer cancel()
	}
	value, _ := a.deliveries.LoadOrStore(incoming.UserID, &userDelivery{})
	delivery := value.(*userDelivery)
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if delivery.messageID != incoming.MessageID {
		delivery.messageID = incoming.MessageID
		delivery.acceptedParts = 0
	}

	if turn, ok := a.turns.Load(incoming.UserID); ok {
		turn.(*typingTurn).stop()
	}
	for _, part := range im.SplitReplyText(text) {
		if wait := time.Until(delivery.last.Add(2 * time.Second)); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		delivery.last = time.Now()
		if err := a.sendText(ctx, incoming, part); err != nil {
			return fmt.Errorf("wechat delivery accepted_parts=%d part_runes=%d: %w", delivery.acceptedParts, len([]rune(part)), err)
		}
		delivery.acceptedParts++
	}
	return nil
}

// SendReply sends a text reply to the user via iLink /ilink/bot/sendmessage API.
func (a *Adapter) SendReply(ctx context.Context, incoming *im.IncomingMessage, reply *im.ReplyMessage) error {
	return a.SendChunk(ctx, incoming, reply.Content)
}

func (a *Adapter) sendText(ctx context.Context, incoming *im.IncomingMessage, text string) error {
	contextToken := ""
	if incoming.Extra != nil {
		contextToken = incoming.Extra["context_token"]
	}

	// Build the send message request matching the iLink protocol
	payload := map[string]interface{}{
		"msg": map[string]interface{}{
			"from_user_id":  "",
			"to_user_id":    incoming.UserID,
			"client_id":     fmt.Sprintf("weknora_%d", time.Now().UnixNano()),
			"message_type":  2, // BOT
			"message_state": 2, // FINISH
			"item_list": []map[string]interface{}{
				{
					"type":      1, // TEXT
					"text_item": map[string]string{"text": text},
				},
			},
			"context_token": contextToken,
		},
		"base_info": newBaseInfo(),
	}

	return a.ilinkPost(ctx, "/ilink/bot/sendmessage", payload)
}

// DownloadFile downloads a media file from the iLink CDN.
// Files are AES-128-ECB encrypted; the key is provided in the message Extra.
func (a *Adapter) DownloadFile(ctx context.Context, msg *im.IncomingMessage) (io.ReadCloser, string, error) {
	if msg.FileKey == "" {
		return nil, "", fmt.Errorf("no file URL in message")
	}
	if err := secutils.ValidateURLForSSRF(msg.FileKey); err != nil {
		return nil, "", fmt.Errorf("file URL rejected by SSRF policy: %w", err)
	}

	fileName := msg.FileName
	if fileName == "" {
		fileName = msg.FileKey
	}

	// Download the file
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, msg.FileKey, nil)
	if err != nil {
		return nil, "", fmt.Errorf("create download request: %w", err)
	}

	resp, err := ilinkHTTPClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("download file: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, "", fmt.Errorf("download failed: status=%d", resp.StatusCode)
	}

	// If no AES key provided, return raw content
	aesKeyB64 := ""
	if msg.Extra != nil {
		aesKeyB64 = msg.Extra["aes_key"]
	}
	if aesKeyB64 == "" {
		return resp.Body, fileName, nil
	}

	// Read and decrypt with AES-128-ECB
	encryptedData, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, "", fmt.Errorf("read encrypted file: %w", err)
	}

	// Parse the AES key: base64 → raw bytes (16) or hex string (32 chars → 16 bytes)
	aesKey, err := parseAESKey(aesKeyB64)
	if err != nil {
		return nil, "", fmt.Errorf("parse aes key: %w", err)
	}

	logger.Debugf(ctx, "[WeChat] Decrypting file: name=%s encrypted_size=%d", fileName, len(encryptedData))

	decrypted, err := decryptAES128ECB(encryptedData, aesKey)
	if err != nil {
		return nil, "", fmt.Errorf("decrypt file: %w", err)
	}

	return io.NopCloser(bytes.NewReader(decrypted)), fileName, nil
}

// parseAESKey decodes an AES key from various formats seen in iLink responses.
//
// Three formats are encountered:
//  1. base64(raw 16 bytes) → CDNMedia.aes_key for file/voice/video
//  2. base64(hex string of 32 chars) → CDNMedia.aes_key (alternative)
//  3. raw hex string (32 chars) → ImageItem.aeskey field (NOT base64-encoded)
//
// The function auto-detects the format and always returns a 16-byte key.
func parseAESKey(aesKeyStr string) ([]byte, error) {
	if aesKeyStr == "" {
		return nil, fmt.Errorf("empty aes key")
	}

	// Case 3: raw hex string (32 hex chars = 16 bytes)
	if len(aesKeyStr) == 32 && isHex(aesKeyStr) {
		return hexDecode(aesKeyStr)
	}

	// Case 1 & 2: base64-encoded
	decoded, err := base64.StdEncoding.DecodeString(aesKeyStr)
	if err != nil {
		decoded, err = base64.RawStdEncoding.DecodeString(aesKeyStr)
		if err != nil {
			// Last resort: maybe it's a hex string of other length
			if isHex(aesKeyStr) && len(aesKeyStr)%2 == 0 {
				return hexDecode(aesKeyStr)
			}
			return nil, fmt.Errorf("cannot decode aes key (len=%d): %w", len(aesKeyStr), err)
		}
	}

	// base64 decoded to exactly 16 raw bytes → direct key
	if len(decoded) == 16 {
		return decoded, nil
	}

	// base64 decoded to 32 ASCII hex chars → parse hex to get 16 bytes
	if len(decoded) == 32 && isHex(string(decoded)) {
		return hexDecode(string(decoded))
	}

	return nil, fmt.Errorf("aes key decoded to %d bytes (expected 16 raw or 32 hex), input len=%d", len(decoded), len(aesKeyStr))
}

// isHex returns true if s contains only hexadecimal characters.
func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return len(s) > 0
}

// hexDecode decodes a hex string to bytes.
func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd-length hex string: %d", len(s))
	}
	result := make([]byte, len(s)/2)
	for i := 0; i < len(result); i++ {
		var b byte
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return nil, fmt.Errorf("hex decode at pos %d: %w", i, err)
		}
		result[i] = b
	}
	return result, nil
}

// ilinkPost sends a POST request to the iLink API with authentication headers.
func (a *Adapter) ilinkPost(ctx context.Context, path string, payload interface{}) error {
	return a.ilinkJSON(ctx, path, payload, nil)
}

func (a *Adapter) ilinkJSON(ctx context.Context, path string, payload interface{}, result any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ilinkBaseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	a.setAuthHeaders(req, body)

	resp, err := ilinkHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("ilink request %s: %w", path, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ilink api %s returned status %d", path, resp.StatusCode)
	}

	var status *struct {
		Ret     int    `json:"ret"`
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(respBody, &status); err != nil || status == nil {
		return fmt.Errorf("ilink api %s: invalid response", path)
	}
	if status.Ret != 0 || status.ErrCode != 0 {
		// Only fixed reason labels may enter logs; arbitrary upstream text may contain secrets.
		reason := "other"
		switch strings.ToLower(strings.TrimSpace(status.ErrMsg)) {
		case "":
			reason = "unspecified"
		case "prepare failed":
			reason = "prepare_failed"
		case "unknown error":
			reason = "unknown_error"
		case "invalid arguments":
			reason = "invalid_arguments"
		case "session timeout":
			reason = "session_timeout"
		case "rate limit", "rate limited":
			reason = "rate_limited"
		}
		return fmt.Errorf("ilink api %s failed: ret=%d errcode=%d reason=%s", path, status.Ret, status.ErrCode, reason)
	}
	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("ilink api %s: invalid response", path)
		}
	}
	return nil
}

// setAuthHeaders sets the required iLink Bot authentication headers.
func (a *Adapter) setAuthHeaders(req *http.Request, body []byte) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("AuthorizationType", "ilink_bot_token")
	if a.botToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.botToken)
	}
	req.Header.Set("X-WECHAT-UIN", generateWeChatUIN())
	if body != nil {
		req.Header.Set("Content-Length", fmt.Sprintf("%d", len(body)))
	}
}

// generateWeChatUIN generates a random X-WECHAT-UIN header value.
// Format: random uint32 → decimal string → base64.
func generateWeChatUIN() string {
	buf := make([]byte, 4)
	_, _ = rand.Read(buf)
	n := binary.BigEndian.Uint32(buf)
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d", n)))
}
