//go:build !windows

package localauth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRejectSharedFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	_, err := NewStore(path, func(context.Context, string) (string, error) { return "", nil })
	requireCode(t, err, "authentication_unavailable")
}
