// Package localauth는 loopback 개발용 MCP 토큰과 사용자별 백엔드 세션을 분리한다.
package localauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/heartblast/ableops-kafka-mcp/internal/ableops"
	"github.com/heartblast/ableops-kafka-mcp/internal/requestctx"
)

const (
	Audience      = "ableops-kafka-mcp"
	DefaultTTL    = time.Hour
	MaxTTL        = 8 * time.Hour
	tokenPrefix   = "ableops_mcp_"
	maxStoreBytes = 1 << 20
	maxEntries    = 1000
)

// VerifyBackend는 캐시 없이 백엔드 세션을 검사하고 현재 사용자 ID를 반환한다.
type VerifyBackend func(context.Context, string) (string, error)

// Error에는 외부 입력이나 내부 오류 원문을 보관하지 않는다.
type Error struct{ Code string }

func (e *Error) Error() string    { return e.Code }
func (e *Error) AuthCode() string { return e.Code }

func authError(code string) error { return &Error{Code: code} }

// Store는 자격증명을 캐시하지 않으며 매 요청마다 보호된 파일을 다시 읽는다.
type Store struct {
	path   string
	verify VerifyBackend
}

type document struct {
	Version  int     `json:"version"`
	Audience string  `json:"audience"`
	Entries  []entry `json:"entries"`
}

type entry struct {
	ClientID     string    `json:"client_id"`
	UserID       string    `json:"user_id"`
	TokenHash    string    `json:"token_sha256"`
	BackendToken string    `json:"backend_token,omitempty"`
	IssuedAt     time.Time `json:"issued_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	Revoked      bool      `json:"revoked"`
}

// NewStore는 시작 시에도 파일 권한과 매핑을 검사한다.
func NewStore(path string, verify VerifyBackend) (*Store, error) {
	if path == "" || verify == nil {
		return nil, authError("authentication_unavailable")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, authError("authentication_unavailable")
	}
	if _, err := readDocument(absolute); err != nil {
		return nil, err
	}
	return &Store{path: absolute, verify: verify}, nil
}

// Authenticate는 MCP audience·만료·폐기를 검사한 뒤 백엔드가 확인한 사용자와 비교한다.
// 호출자의 MCP 토큰은 백엔드에 전달하지 않는다.
func (s *Store) Authenticate(ctx context.Context, bearer string) (requestctx.Principal, error) {
	if !validMCPToken(bearer) {
		return requestctx.Principal{}, authError("authentication_required")
	}
	doc, err := readDocument(s.path)
	if err != nil {
		return requestctx.Principal{}, err
	}
	sum := sha256.Sum256([]byte(bearer))
	var matched *entry
	for i := range doc.Entries {
		digest, _ := hex.DecodeString(doc.Entries[i].TokenHash)
		if subtle.ConstantTimeCompare(sum[:], digest) == 1 {
			matched = &doc.Entries[i]
		}
	}
	now := time.Now()
	if matched == nil || matched.Revoked || !now.Before(matched.ExpiresAt) || now.Before(matched.IssuedAt) {
		return requestctx.Principal{}, authError("authentication_required")
	}
	principal, _ := requestctx.FromContext(ctx)
	principal.UserID, principal.ClientID = matched.UserID, matched.ClientID
	principal.BackendToken, principal.MCPToken = matched.BackendToken, bearer
	userID, err := s.verify(requestctx.WithPrincipal(ctx, principal), matched.BackendToken)
	if err != nil {
		return requestctx.Principal{}, backendError(err)
	}
	if userID != matched.UserID || userID == "" {
		return requestctx.Principal{}, authError("authentication_required")
	}
	return principal, nil
}

func backendError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return authError("timeout")
	}
	if errors.Is(err, context.Canceled) {
		return authError("canceled")
	}
	var backend *ableops.Error
	if errors.As(err, &backend) {
		switch backend.Code {
		case "authentication_required":
			return authError("backend_authentication_required")
		case "access_denied", "timeout", "canceled":
			return authError(backend.Code)
		}
	}
	return authError("backend_unavailable")
}

func readDocument(path string) (document, error) {
	file, err := openPrivate(path)
	if err != nil {
		return document{}, authError("authentication_unavailable")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxStoreBytes+1))
	if err != nil || len(data) > maxStoreBytes {
		return document{}, authError("authentication_unavailable")
	}
	var doc document
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&doc) != nil || decoder.Decode(new(any)) != io.EOF || validateDocument(doc) != nil {
		return document{}, authError("authentication_unavailable")
	}
	return doc, nil
}

func validateDocument(doc document) error {
	invalid := authError("authentication_unavailable")
	if doc.Version != 1 || doc.Audience != Audience || len(doc.Entries) > maxEntries {
		return invalid
	}
	hashes, clients, users := map[string]bool{}, map[string]bool{}, map[string]string{}
	for _, value := range doc.Entries {
		digest, err := hex.DecodeString(value.TokenHash)
		if err != nil || len(digest) != sha256.Size || value.TokenHash != strings.ToLower(value.TokenHash) ||
			!validID(value.ClientID) || !validID(value.UserID) || hashes[value.TokenHash] || clients[value.ClientID] ||
			value.IssuedAt.IsZero() || !value.ExpiresAt.After(value.IssuedAt) || value.ExpiresAt.Sub(value.IssuedAt) > MaxTTL {
			return invalid
		}
		hashes[value.TokenHash], clients[value.ClientID] = true, true
		if value.Revoked && value.BackendToken == "" {
			continue
		}
		if !validBackendToken(value.BackendToken) {
			return invalid
		}
		if previous, found := users[value.BackendToken]; found && previous != value.UserID {
			return invalid
		}
		users[value.BackendToken] = value.UserID
	}
	return nil
}

func validID(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, ch := range value {
		if ch < 0x20 || ch == 0x7f {
			return false
		}
	}
	return true
}

func validBackendToken(value string) bool {
	if value == "" || len(value) > 4096 || strings.HasPrefix(value, tokenPrefix) {
		return false
	}
	for _, ch := range value {
		if ch < 0x21 || ch > 0x7e {
			return false
		}
	}
	return true
}

func validMCPToken(value string) bool {
	if !strings.HasPrefix(value, tokenPrefix) {
		return false
	}
	raw := strings.TrimPrefix(value, tokenPrefix)
	data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	return err == nil && len(data) == 32 && base64.RawURLEncoding.EncodeToString(data) == raw
}

// Enroll은 관리자 CLI 전용이다. 사용자 ID는 백엔드에서만 얻으며 원문 MCP 토큰은
// 덮어쓰기가 금지된 소유자 전용 파일에만 저장한다. 반환값은 사용자 ID뿐이다.
func Enroll(ctx context.Context, path, clientID, backendToken string, ttl time.Duration, tokenOutput string, verify VerifyBackend) (string, error) {
	if path == "" || tokenOutput == "" || !validID(clientID) || !validBackendToken(backendToken) ||
		ttl <= 0 || ttl > MaxTTL || verify == nil {
		return "", authError("invalid_enrollment")
	}
	storePath, pathErr := filepath.Abs(path)
	outputPath, outputErr := filepath.Abs(tokenOutput)
	if pathErr != nil || outputErr != nil || strings.EqualFold(storePath, outputPath) {
		return "", authError("invalid_enrollment")
	}
	userID, err := verify(ctx, backendToken)
	if err != nil {
		return "", backendError(err)
	}
	if !validID(userID) {
		return "", authError("invalid_enrollment")
	}
	unlock, err := lockStore(path)
	if err != nil {
		return "", err
	}
	defer unlock()
	doc := document{Version: 1, Audience: Audience}
	if _, err := os.Lstat(path); err == nil {
		doc, err = readDocument(path)
		if err != nil {
			return "", err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", authError("authentication_unavailable")
	}
	for _, value := range doc.Entries {
		if value.ClientID == clientID && !value.Revoked {
			return "", authError("client_already_registered")
		}
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return "", authError("authentication_unavailable")
	}
	token := tokenPrefix + base64.RawURLEncoding.EncodeToString(random)
	digest := sha256.Sum256([]byte(token))
	now := time.Now().UTC()
	value := entry{ClientID: clientID, UserID: userID, TokenHash: hex.EncodeToString(digest[:]), BackendToken: backendToken, IssuedAt: now, ExpiresAt: now.Add(ttl)}
	// 폐기한 클라이언트 ID는 새 토큰으로 재등록할 수 있으며 이전 해시는 제거한다.
	for i := range doc.Entries {
		if doc.Entries[i].ClientID == clientID {
			doc.Entries = append(doc.Entries[:i], doc.Entries[i+1:]...)
			break
		}
	}
	doc.Entries = append(doc.Entries, value)
	if err := validateDocument(doc); err != nil {
		return "", err
	}
	output, err := createPrivate(tokenOutput)
	if err != nil {
		return "", authError("token_output_unavailable")
	}
	complete := false
	defer func() {
		output.Close()
		if !complete {
			_ = os.Remove(tokenOutput)
		}
	}()
	if _, err := io.WriteString(output, token+"\n"); err != nil {
		return "", authError("token_output_unavailable")
	}
	if err := output.Sync(); err != nil {
		return "", authError("token_output_unavailable")
	}
	if err := output.Close(); err != nil {
		return "", authError("token_output_unavailable")
	}
	if err := writeDocument(path, doc); err != nil {
		return "", err
	}
	complete = true
	return userID, nil
}

// Revoke는 다음 HTTP 요청부터 폐기를 적용하고 저장된 백엔드 토큰도 지운다.
func Revoke(path, clientID string) error {
	if path == "" || !validID(clientID) {
		return authError("invalid_enrollment")
	}
	unlock, err := lockStore(path)
	if err != nil {
		return err
	}
	defer unlock()
	doc, err := readDocument(path)
	if err != nil {
		return err
	}
	for i := range doc.Entries {
		if doc.Entries[i].ClientID == clientID {
			doc.Entries[i].Revoked = true
			doc.Entries[i].BackendToken = ""
			return writeDocument(path, doc)
		}
	}
	return authError("client_not_found")
}

func lockStore(path string) (func(), error) {
	lock, err := createPrivate(path + ".lock")
	if err != nil {
		return nil, authError("store_locked_or_unavailable")
	}
	return func() { _ = lock.Close(); _ = os.Remove(path + ".lock") }, nil
}

func writeDocument(path string, doc document) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil || len(data) > maxStoreBytes {
		return authError("authentication_unavailable")
	}
	suffix := make([]byte, 12)
	if _, err := rand.Read(suffix); err != nil {
		return authError("authentication_unavailable")
	}
	temporary := path + ".tmp-" + hex.EncodeToString(suffix)
	file, err := createPrivate(temporary)
	if err != nil {
		return authError("authentication_unavailable")
	}
	defer func() { _ = file.Close(); _ = os.Remove(temporary) }()
	if _, err := file.Write(data); err != nil {
		return authError("authentication_unavailable")
	}
	if err := file.Sync(); err != nil {
		return authError("authentication_unavailable")
	}
	if err := file.Close(); err != nil {
		return authError("authentication_unavailable")
	}
	// Root.Rename은 Windows에서도 삭제 공유 핸들이 열린 기존 파일을 교체한다.
	// 임시 파일과 대상 파일은 같은 디렉터리에 두어 완성된 문서만 공개한다.
	directory, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return authError("authentication_unavailable")
	}
	defer directory.Close()
	if err := directory.Rename(filepath.Base(temporary), filepath.Base(path)); err != nil {
		return authError("authentication_unavailable")
	}
	return nil
}

func openPrivate(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, authError("authentication_unavailable")
	}
	file, err := openPrivateFile(path)
	if err != nil {
		return nil, authError("authentication_unavailable")
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || checkPrivate(file) != nil {
		file.Close()
		return nil, authError("authentication_unavailable")
	}
	return file, nil
}
