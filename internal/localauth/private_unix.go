//go:build !windows

package localauth

import (
	"os"
	"syscall"
)

func createPrivate(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
}

func openPrivateFile(path string) (*os.File, error) { return os.Open(path) }

func checkPrivate(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return authError("authentication_unavailable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() || info.Mode().Perm()&0077 != 0 {
		return authError("authentication_unavailable")
	}
	return nil
}
