// Package extension은 AbleOps Core 가 관리하는 **외부 프로세스 Extension** 으로서의 MCP 실행
// 방식을 구현한다.
//
// 이 패키지는 MCP 기능을 새로 만들지 않는다. standalone CLI 와 **같은** internal/app.Runtime
// 을 조립하고, 그 Runtime 이 만든 listener 없는 HTTP Handler 를 AbleOps SDK 의 extserver 가
// 여는 단일 loopback listener 에 얹을 뿐이다. 도구 목록·스키마·Dynamic 승격·Backend REST
// 계약은 standalone 과 동일하다.
//
// 이 패키지가 책임지는 것은 셋뿐이다.
//
//   - Core 가 준 HostURL 에서 Backend REST origin 을 만든다(origin.go)
//   - Managed 모드 전용 설정과 프로세스 내부 공유 비밀을 만든다(config.go)
//   - extensionv1.Extension 생애주기를 Runtime lifecycle 에 잇는다(extension.go)
package extension

import (
	_ "embed"
	"sync"

	extv1 "github.com/heartblast/ableops-sdk/extension/v1"
)

// manifestYAML은 이 Extension 의 선언 파일이다.
// 바이너리에 포함하므로 배포 후 파일 유실·위변조로 Extension 이 조용히 다른 권한을 갖는 일이 없다.
//
//go:embed manifest.yaml
var manifestYAML []byte

// ID는 이 Extension 의 식별자다. manifest.yaml 의 id 와 반드시 같아야 하며 테스트가 이를 고정한다.
//
// ⚠ 이 값은 Core 의 환경변수(ABLEOPS_EXT_ID)와 교차 검증된다 — 다르면 잘못된 바이너리를
// 실행한 것이므로 SDK 가 기동을 거부한다.
const ID = "ableops-kafka-mcp"

// Manifest 파싱은 프로세스당 1회만 수행한다(호출마다 같은 값을 돌려주어야 한다는 SDK 계약).
var (
	manifestOnce sync.Once
	manifestVal  extv1.Manifest
	manifestErr  error
)

// LoadManifest는 embed 된 manifest.yaml 을 1회 파싱해 돌려준다.
//
// 반환된 Manifest 의 슬라이스는 공유되므로 **호출자는 반환값을 변경하지 않는다**(Manifest 는 불변).
func LoadManifest() (extv1.Manifest, error) {
	manifestOnce.Do(func() {
		manifestVal, manifestErr = extv1.ParseManifest(manifestYAML)
	})
	return manifestVal, manifestErr
}
