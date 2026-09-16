//go:build windows

package localauth

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows의 os.Chmod(0600)은 ACL을 제한하지 않으므로 생성 시점부터 상속을 차단한다.
func createPrivate(path string) (*os.File, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	descriptor, err := windows.SecurityDescriptorFromString("O:" + user.User.Sid.String() + "D:P(A;;FA;;;" + user.User.Sid.String() + ")")
	if err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	attributes := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: descriptor}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE, 0, &attributes, windows.CREATE_NEW, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

// 읽는 동안에도 폐기 명령이 새 저장소 파일을 교체할 수 있도록 삭제 공유를 허용한다.
// 열린 핸들은 이전 스냅샷을 읽고 다음 요청은 교체된 파일의 권한부터 다시 검사한다.
func openPrivateFile(path string) (*os.File, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

func checkPrivate(file *os.File) error {
	invalid := authError("authentication_unavailable")
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return invalid
	}
	descriptor, err := windows.GetSecurityInfo(windows.Handle(file.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return invalid
	}
	owner, _, err := descriptor.Owner()
	if err != nil || owner == nil || !owner.Equals(user.User.Sid) {
		return invalid
	}
	control, _, err := descriptor.Control()
	if err != nil || control&windows.SE_DACL_PROTECTED == 0 {
		return invalid
	}
	dacl, _, err := descriptor.DACL()
	if err != nil || dacl == nil || dacl.AceCount == 0 {
		return invalid
	}
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if windows.GetAce(dacl, i, &ace) != nil || ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return invalid
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !sid.Equals(user.User.Sid) {
			return invalid
		}
	}
	return nil
}
