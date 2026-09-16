//go:build windows

package localauth

import (
	"context"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestRejectWindowsSharedFileACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	enrollFixture(t, path, "client-a", "synthetic-backend-a", "user-a")
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;" + user.User.Sid.String() + ")(A;;FR;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, dacl, nil); err != nil {
		t.Fatal(err)
	}
	_, err = NewStore(path, func(context.Context, string) (string, error) { return "", nil })
	requireCode(t, err, "authentication_unavailable")
}
