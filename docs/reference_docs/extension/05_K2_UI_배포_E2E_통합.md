# AbleOps Kafka — Phase K2: Managed MCP UI·배포·E2E 통합

## 대상 저장소

`heartblast/ableops-kafka`

필요 시 패키지 산출물 확인을 위해 `heartblast/ableops-kafka-mcp`의 최신 Extension package 구조를 참고한다.

## 선행조건

Phase K1이 완료되어 다음이 동작해야 한다.

```text
AbleOps Kafka
  → Managed MCP Extension Target 자동 해소
  → Delegation 발급
  → /mcp
  → tools/list
  → tools/call
```

기존 External MCP fallback도 유지되어 있어야 한다.

## 목적

Managed MCP Extension 연동을 실제 제품 운영 흐름에 마무리한다.

이번 Phase는 새로운 MCP 기능 개발이 아니라:

- AI 설정 화면
- Extension 운영 화면
- 설치/배포 흐름
- 상태 표현
- E2E 회귀 검증

을 정리하는 작업이다.

---

## 1. AI 설정 화면 개선

현재 AI 설정 화면의 기존 구조를 최대한 유지한다.

Managed MCP 사용 시 다음 정보를 읽기 전용 상태로 표시한다.

예:

```text
MCP 실행 방식 : AbleOps Managed Extension
Extension      : ableops-kafka-mcp
상태           : RUNNING
MCP 연결       : 정상
노출 Tool      : 33개
```

실제 Tool 수는 `tools/list` 결과를 사용하고 하드코딩하지 않는다.

### Managed Mode에서 숨기거나 비활성화할 설정

Managed MCP가 사용 중일 때 사용자가 다음 값을 직접 편집할 필요가 없어야 한다.

- MCP URL
- MCP fixed port
- MCP Delegation URL
- MCP Delegation Shared Secret

현재 MCP URL이 보안상 읽기 전용인 구조라면 그 원칙을 유지하면서 Managed mode에서는 "Extension에서 자동 관리"로 표현한다.

### External MCP

기존 External MCP 설정 방식은 계속 표시/지원한다.

Managed와 External을 UI에서 명확히 구분한다.

---

## 2. 상태 표시

다음 상태를 혼동 없이 표시한다.

```text
Extension 미설치
Extension 비활성
Extension 시작 중
Extension 정상
Extension DEGRADED
Extension FAILED
MCP protocol 오류
Delegation 오류
LLM 오류
Portal Session 오류
```

특히:

```text
MCP 장애 → 로그인 세션 만료
```

와 같이 잘못 매핑하지 않는다.

상태 판정은 새 Frontend 로직에서 추측하지 말고 Backend가 제공하는 정규화된 상태를 사용한다.

---

## 3. Extension 관리 재사용

새 MCP 전용 프로세스 관리 화면을 만들지 않는다.

기존 AbleOps Extension 관리 기능을 그대로 사용한다.

목표 Lifecycle:

```text
Extension Enable
  → MCP process 시작

Extension Disable
  → MCP process 종료

MCP process crash
  → 기존 Extension supervisor 재시작

AbleOps Kafka 종료
  → MCP child process 종료

Extension Update
  → 기존 Extension update lifecycle 사용
```

MCP만을 위한 별도 systemd, Windows Service, PID 관리 기능을 만들지 않는다.

---

## 4. 배포 구조

새 installer를 만들지 않는다.

기존 AbleOps Extension 설치 체계를 그대로 사용한다.

Phase M3에서 생성된:

```text
ableops-kafka-mcp-<version>.ableops-ext
```

형태의 package를 기존 설치/카탈로그 구조로 배포할 수 있게 한다.

현재 Extension 정책을 확인한 뒤 다음 둘 중 적절한 방식을 선택한다.

### 선택 A — 기본 번들 제공

AbleOps Kafka 설치 package에 MCP Extension package를 함께 제공하되:

- Core binary에 링크하지 않음
- 첫 설치 시 자동 설치 여부는 정책에 따름
- enable/disable 가능

### 선택 B — 선택 설치

Extension Catalog 또는 관리 UI에서 별도 설치.

어느 방식을 채택하든:

- MCP binary를 Core binary에 합치지 않는다.
- MCP를 별도 OS service로 등록하지 않는다.
- Core ExtensionHost가 lifecycle을 관리한다.

결정 이유를 완료 보고에 남긴다.

---

## 5. 기존 설정 정리

Managed MCP 사용 시 불필요해진 설정을 식별한다.

예:

```text
MCP_DELEGATION_URL
MCP_DELEGATION_SECRET
별도 MCP :8081 실행 설정
별도 MCP 서비스 기동 스크립트
```

단, External MCP fallback 때문에 설정 계약 자체를 바로 삭제하지 않는다.

다음처럼 분류한다.

```text
Managed mode에서 불필요
External mode에서 계속 지원
향후 deprecated 가능
```

문서도 동일하게 정리한다.

---

## 6. 운영 관점 개선

가능하면 기존 AI 운영현황 또는 Extension 상태 화면에서 다음 정도만 확인할 수 있게 한다.

- Managed/External mode
- Extension state
- MCP reachable
- Tool count
- 최근 연결 테스트 결과

다음은 표시하지 않는다.

- MCP Token
- Delegation Token
- Call Token
- Backend Session
- Internal Secret
- raw Tool argument
- raw Tool result
- 내부 loopback URL 전체

loopback port 자체도 운영상 필요하지 않으면 UI에 노출하지 않는다.

---

## 7. E2E 검증

실제 Kafka 변경 작업 없이 mock/synthetic 환경에서 다음 시나리오를 검증한다.

### 정상 경로

```text
AbleOps Kafka 시작
  ↓
MCP Extension 자동 시작
  ↓
Extension RUNNING
  ↓
사용자 로그인
  ↓
AI Chat 질문
  ↓
MCP Delegation
  ↓
tools/list
  ↓
tools/call
  ↓
LLM 최종 응답
```

### 장애 경로

#### MCP crash

```text
MCP Extension process 종료
  ↓
Extension supervisor 감지
  ↓
재기동
  ↓
새 port / Call Token
  ↓
다음 Chat 요청 정상
```

#### Extension Disable

```text
Extension Disable
  ↓
MCP process 종료
  ↓
AI status = disabled/unavailable
  ↓
사용자 세션 만료로 오표시하지 않음
```

#### Portal 종료

Core 종료 후 MCP child process가 남지 않아야 한다.

#### External fallback

Managed MCP가 설치되지 않은 환경에서 기존 External MCP 설정이 정상 동작해야 한다.

---

## 8. 회귀 테스트

반드시 기존 다음 기능의 회귀를 확인한다.

- Extension 설치
- Extension enable/disable
- Extension update
- Extension supervisor
- AI Settings
- AI Status
- AI Chat
- AI Chat Stream
- MCP Delegation
- context/provenance
- AI 감사로그
- External MCP

변경 패키지 targeted test를 먼저 수행하고 마지막에 full verification을 1회 수행한다.

---

## 금지사항

- Core와 MCP를 하나의 바이너리로 합치기
- MCP 별도 system service 추가
- 새 process supervisor 구현
- Tool 계약 변경
- Tool 추가/삭제
- 인증 전체 리팩터링
- OAuth/OIDC 신규 도입
- Browser에 MCP Token 전달
- Browser에서 MCP 직접 연결
- loopback Call Token UI 노출
- 실제 운영 Kafka 변경
- 원격 push

---

## 완료 조건

1. AbleOps Kafka 하나를 시작하면 Managed MCP가 함께 기동된다.
2. MCP 장애가 Core 장애로 전파되지 않는다.
3. 기존 Extension supervisor가 MCP를 관리한다.
4. Managed mode에서는 별도 MCP URL/port/delegation secret 설정이 필요 없다.
5. 기존 External MCP도 계속 사용할 수 있다.
6. AI 설정/상태 화면에서 Managed/External 방식이 명확히 구분된다.
7. MCP restart 후 다음 Chat이 정상 동작한다.
8. Portal 종료 후 MCP child process가 남지 않는다.
9. Token/Secret이 화면·로그·응답에 노출되지 않는다.

## 완료 보고

다음만 간단히 정리한다.

1. UI 변경사항
2. 배포 방식
3. Managed mode에서 제거된 운영 설정
4. External mode에 남은 legacy 설정
5. E2E 테스트 결과
6. 장애 복구 검증 결과
7. 실환경에서 추가 검증할 항목
