# gpt2omo Secure MCP Tunnel 전환 · 보안 격상 전체 구현 계획서

- 문서 버전: v1.0 (2026-09-13)
- 대상 저장소: `/Users/indo/code/project/omo-bridge` (gpt2omo v0.7.0, Rust)
- 연관 문서: `docs/local-bridge-supervision.md` (launchd), `docs/security-performance-audit.md` (S1/S2/P1), `README.md`
- 상태: 계획 (구현 전)

---

## 0. 요약

현재 `gpt2omo` 브리지는 `cloudflared` 인바운드 터널(`code.checka.cc`)로 공개 엔드포인트를 노출하고 있고,
전송 계층 인증이 없어 `scope_id`(UUID)가 유일한 방어선이다. 이는 (1) 무인증 공개 노출,
(2) cloudflared 530/엣지 QUIC 끊김 장애 클래스라는 두 가지 구조적 약점을 안고 있다.

본 계획은 이를 **OpenAI 공식 Secure MCP Tunnel(아웃바운드 `tunnel-client`)** 로 전환하고,
터널 전환과 무관하게 남는 S1 체인(무인증·와일드카드 CORS·`scope_id` 유출)과
S2(`run_command` 화이트리스트 부재)를 격상시키는 것을 목표로 한다.

**gpt2omo 코드 변경은 최소화한다.** 브리지는 계속 `127.0.0.1:18800` 루프백 HTTP 서버로 동작하고,
pinned `tunnel-client` 바이너리가 long-poll로 요청을 포워딩한다. 코드가 아니라 **배포·전송 계층의 전환**이다.

```
[현재 - cloudflared 인바운드, 무인증]
ChatGPT 커넥터 ──공개 URL──> Cloudflare 엣지 ──> cloudflared ──> gpt2omo (127.0.0.1:18800)
             code.checka.cc    [scope_id UUID = 유일한 방어선, 530/엣지 QUIC 끊김]

[목표 - OpenAI Secure MCP Tunnel, 아웃바운드]
ChatGPT ──> OpenAI 터널 엔드포인트 (tunnel_id, 워크스페이스 연결)
                   ↑ long-poll (아웃바운드)
            tunnel-client ──127.0.0.1:18800──> gpt2omo
```

---

## 1. 범위 (구현 항목)

| # | 항목 | 분류 | 우선순위 |
|---|---|---|---|
| 1 | Secure MCP Tunnel 경로 구현 (outbound `tunnel-client`) | 전송 계층 전환 | **P0** |
| 2 | S1 체인 격상 (필수 토큰·CORS 축소·`scope_id` 마스킹·capability_secret) | 심층 방어 | **P0** |
| 3 | ~~S2 격상: `run_command` 바이너리 화이트리스트~~ | 감사 Critical | **완료됨** (2026-09-13 코드 확인 — 감사 이후 구현됨, §4 참고) |
| 4 | P1 격상: 블로킹 도구 실행의 `spawn_blocking`/async 전환 | 감사 Critical | **P1** |

터널 전환(1)이 성공해도 로컬 바인딩·미래의 재노출 가능성 때문에 (2)는 **독립적으로 유지**한다.
터널은 전송 계층을 닫는 것이지 브리지 내부 방어선을 대체하지 않는다.

---

## 2. Phase 1 — Secure MCP Tunnel 경로 구현 (P0)

### 2.1 대상 구조

- MCP 서버: `gpt2omo` (`src/server.rs`), 루프백 `127.0.0.1:18800` 바인딩 유지
- 터널 클라이언트: **OpenAI 공식 오픈소스 바이너리** [openai/tunnel-client](https://github.com/openai/tunnel-client) 최신 릴리스 (CoS 배포판에 포함된 것과 동일한 공식 클라이언트)
- 브리지 측 코드 변경: **없음** (루프백 HTTP 서버 그대로)

### 2.2 단계

#### Step 1.1 — Platform 터널 등록
- [Platform → Tunnels](https://platform.openai.com/settings/organization/tunnels)에서 터널 생성 (`tunnel_id` 확보)
- 제한 API 키 생성: 권한 **Tunnels Read + Use** (Manage 불필요 — 읽기/실행 전용 키를 클라이언트에 둔다)
- **검증 가능한 완료 조건**: 터널이 Platform 목록에 존재 + API 키가 발급됨

#### Step 1.2 — `tunnel-client` 상주 (launchd 에이전트)
- pinned 바이너리: 최신 릴리스 URL 고정 (runbook에 명시, 하드코딩된 특정 버전 URL 금지)
- 프로파일: HTTP MCP 서버 모드로 `--mcp-server-url http://127.0.0.1:18800/mcp`
- launchd 에이전트: `com.omo.gpt2omo.tunnel` — `RunAtLoad`, `KeepAlive` (`docs/local-bridge-supervision.md`의 bridge/relay/Chrome 에이전트와 동일 패턴)
- 로그: `~/Library/Logs/gpt2omo-tunnel.{out,err}.log`
- **검증 가능한 완료 조건**:
  - `curl http://127.0.0.1:<tunnel-client-port>/healthz` 및 `/readyz` → 200
  - `tunnel-client doctor --profile <name> --explain` → 통과
  - 재부팅 후에도 에이전트가 살아있음 (launchd 로그 확인)

#### Step 1.3 — ChatGPT 커넥터 연결 (계정별)
- chatgpt.com/plugins에서 개발자 모드 앱 생성 시 **Connection 유형 = Tunnel** 선택, 터널 선택
- 터널에 **모든 대상 ChatGPT 워크스페이스를 연결(associate)** — 터널은 여러 워크스페이스에 연결 가능하며, 연결되지 않은 워크스페이스에서는 앱 목록에 안 보임
- **검증 가능한 완료 조건**: 각 ChatGPT 계정에서 앱이 보이고, 도구 목록이 정상 로드됨 (`tools/list` 18툴 유지)

#### Step 1.4 — 병행 운영 검증 (전환 게이트)
- 기존 `code.checka.cc` 경로와 새 터널 경로를 **병행**으로 두고 실제 워커 디스패치로 검증
- 검증 항목:
  - (a) `read_file` / `run_command` / `completion_check` 등 핵심 툴 왕복 정상
  - (b) 60s 커넥터 타임아웃 내 응답 (long-poll 지연 관측·기록)
  - (c) 대량 출력 커맨드(링버퍼 10MB급) 정상 전달
  - (d) SSE 스트림(`/events`)이 터널 경로에서 필요 없음을 확인 (relay는 로컬 유지)
- **검증 가능한 완료 조건**: (a)~(d) 모두 통과, 기존 경로와의 레이턴시 차이 기록

#### Step 1.5 — cloudflared 경로 은퇴
- Step 1.4 통과 후 `code.checka.cc` cloudflared 프로세스 중단 (`docs/local-bridge-supervision.md`의 tunnel 항목 갱신)
- **롤백 경로 유지**: cloudflared 설정은 삭제하지 않고 보존 — 터널 장애 시 수동으로 되돌릴 수 있게 함
- README의 `cloudflared tunnel --url ...` 안내 문단을 터널 절차로 교체 (S3 문서 정정과 병행)

### 2.3 리스크 & 유의점

| 리스크 | 완화 |
|---|---|
| long-poll 왕복 지연 추가 | Step 1.4(b)에서 실측; 60s 타임아웃 내 여유 확인 |
| 워크스페이스 연결 누락 → 앱 목록 미표시 | Step 1.3에서 계정별 연결 확인; 문서에 연결 절차 명시 |
| `tunnel-client` 미실행 시 전체 도구 호출 실패 | launchd `KeepAlive` + `/healthz` 모니터링 + 기존 경로 롤백 유지 |
| 공개 플러그인 제출 불가 (터널은 개발자 모드 전용) | 우리는 개발자 모드만 사용 — 해당 없음 |
| 18툴 캡·safety-scan 블록 등 커넥터 자체 동작 | 터널과 무관 — 기존과 동일하게 유지됨을 기대, Step 1.4(a)에서 확인 |

---

## 3. Phase 2 — S1 체인 격상 (P0, 터널과 독립)

터널 전환으로 공개 노출은 사라지지만, **브리지 자체의 로컬 방어선은 그대로 유지**한다.
감사 보고서 `docs/security-performance-audit.md` §S1의 해결 방안을 구현한다.

### 3.1 구현 항목

| 항목 | 대상 파일 | 내용 |
|---|---|---|
| 필수 토큰 | `src/cli.rs`, `src/server.rs` | `--token` 미지정 시 기동 시점에 32바이트 랜덤 토큰 생성 → `~/.omo/bridge/token` (0600) 저장 + stdout 1회 출력. 무인증은 명시적 `--insecure-no-auth`로만 허용 |
| CORS 축소 | `src/server.rs` (`CorsLayer`) | `allow_origin(Any)` 제거 → 명시 허용리스트 (로컬 오리진만), `allow_credentials(false)` 유지 |
| Host 검증 유지 | `src/server.rs` (기존 `host_headers...` 테스트) | 이미 구현됨 — 회귀 테스트 유지 |
| `scope_id` 마스킹 | `src/server.rs` (SSE 이벤트), `src/events.rs` | `tool_started` 등 SSE 이벤트에서 `scope_id`·절대 워크스페이스 경로 제거/마스킹. relay는 데몬과 동일 신뢰 도메인이므로 인증된 채널로 실제 scope 조회 |
| capability_secret | 스코프 파일 + `src/server.rs` | 스코프별 랜덤 32바이트 `capability_secret` 추가, 툴 호출 시 `scope_id`와 함께 요구. 이벤트 유출만으로 스코프 탈취 불가하게 격상 |

### 3.2 검증 가능한 완료 조건

- `curl http://127.0.0.1:18800/healthz` (Authorization 없음) → **401**
- `curl http://127.0.0.1:18800/healthz` (올바른 토큰) → **200**
- `curl -H 'Origin: https://evil.example' http://127.0.0.1:18800/events` → `scope_id` 유출 0건 (감사 PoC의 재현 스크립트가 실패해야 함)
- 유출된 `scope_id`만으로 `run_command` 호출 → **거부** (capability_secret 없이는 불가)
- `cargo test`에서 기존 `verify_auth_enforces_bearer_token`, `host_headers...` 회귀 테스트 유지·확장

---

## 4. ~~Phase 3 — S2: `run_command` 바이너리 화이트리스트~~ → **이미 구현됨 (계획에서 제외)**

감사 보고서(2026-08-18, v0.7.0)는 화이트리스트 부재를 Critical(S2)로 보고했으나, 2026-09-13 코드 확인 결과
**감사 이후 구현이 완료되어 있다**:

- `src/tools/run_command.rs:14` `ALLOWED_BINARIES` (cargo, rustc, npm, pnpm, yarn, bun, bunx, node, python,
  python3, pytest, uv, go, make, git, vitest, jest, tsc, biome, ruff, sg, ast-grep)
- `:25` `BLOCKED_SHELL_WRAPPERS` (sh, bash, zsh, env, xargs, eval, perl, ruby, awk, script, sudo, su, pwsh 등)
- `:192` `validate_git_args` — `-c`, `--exec-path`, `--upload-pack`, `--receive-pack`, `--config-env` 거부
  (감사가 경고한 `git -c core.pager='sh -c ...'` 우회 차단)
- `--allow-arbitrary-commands` / `OMO_BRIDGE_ALLOW_ARBITRARY_COMMANDS` 옵트인 탈출구 (기본 거부)
- 테스트: `test_prepare_command_blocks_shell_wrappers_and_disallowed_binaries` — `sh -c`, `/bin/bash`, `env`,
  `xargs`, `eval`, `perl`, `ruby`, `awk`, `script`, `curl`, `rm` 거부 단언

남는 것: 없음. SECURITY.md와 코드 일치도 이로써 회복된 상태이므로 본 계획의 구현 대상에서 제외한다.

---

## 5. Phase 4 — P1: 블로킹 도구 실행 비동기화 (P1)

감사 §P1: 모든 도구 실행이 Tokio 워커를 블로킹 (롱폴 40건 동시 시 `/healthz` 27.9s, 계측 완료).

### 5.1 구현 (단계적)

1. `dispatch_tool`을 `async fn`으로 전환, 동기 본문을 `tokio::task::spawn_blocking`으로 감싼다 (최소 변경, 즉시 효과)
2. `CommandManager`의 `Mutex`+`Condvar` → `tokio::sync::Mutex` + `Notify` (롱폴을 진짜 비동기 대기로)
3. 서브프로세스(`git`, `sg`, LSP)를 `tokio::process::Command`로 전환 + `tokio::time::timeout`
4. `Builder::max_blocking_threads` 명시 + 스코프별 동시 툴 호출 세마포어

### 5.2 검증 가능한 완료 조건

- 감사 재현 스크립트: 롱폴 40건 동시 진행 중 `/healthz` 응답 → **100ms 이내**
- `sleep 120` 커맨드 1건의 `run_command`가 HTTP 요청을 15초간 잡아두지 않음 (즉시 `detached_running` 반환)

---

## 6. 마이그레이션 & 롤백

### 6.1 순서 (터널이 먼저, 코드 격상이 뒤따름)

```
Phase 1 (터널)  →  병행 검증  →  Phase 2~4 (코드 격상)  →  cloudflared 은퇴
```

- Phase 1을 먼저 하는 이유: 터널 전환은 코드 변경이 없으므로 **롤백이 가장 쉽고**, 성공 시 공개 노출이 즉시 사라져 이후 코드 격상의 긴급성이 낮아진다.
- Phase 2~4는 터널 성공 여부와 무관하게 진행한다 (로컬 바인딩·재노출 방지를 위한 심층 방어).

### 6.2 롤백 경로

- 터널: launchd 에이전트 `com.omo.gpt2omo.tunnel` 언로드 → cloudflared 프로세스 재기동 (`code.checka.cc` 설정 보존)
- Phase 2: `--insecure-no-auth` 플래그로 기존 동작 보존 (문서화된 명시적 탈출구)
- Phase 3: `--allow-arbitrary-commands` 플래그로 기존 동작 보존
- Phase 4: 단계별 커밋으로 부분 롤백 가능

---

## 7. 일정 (제안)

| 단계 | 내용 | 예상 규모 |
|---|---|---|
| 1 | Phase 1 Step 1.1~1.2 (Platform 등록 + launchd 에이전트) | 반나절 |
| 2 | Phase 1 Step 1.3~1.4 (계정 연결 + 병행 검증) | 반나절~하루 |
| 3 | Phase 2 (S1 격상) | 하루 |
| 4 | Phase 4 (P1 비동기화) | 1~2일 |
| 5 | 문서 정리 (README, supervision, SECURITY) | 반나절 |

---

## 8. 완료 정의 (Definition of Done)

- [ ] `tunnel-client`가 launchd로 상주, `/healthz`+`/readyz` 200, `doctor` 통과
- [ ] 각 ChatGPT 계정에서 Tunnel 유형 앱 등록, 도구 목록 18툴 로드 확인
- [ ] 병행 검증 4항목(a~d) 통과, 레이턴시 차이 기록
- [ ] `code.checka.cc` cloudflared 경로 은퇴 (설정 보존, 롤백 경로 문서화)
- [ ] Authorization 없는 `/healthz` → 401, 토큰 있으면 200
- [ ] 외부 Origin `/events`에서 `scope_id` 유출 0건 (감사 PoC 재현 실패)
- [x] `sh -c` 및 `git -c core.pager=...` 거부 확인 (2026-09-13 — 기구현·테스트 존재 확인, `src/tools/run_command.rs`)
- [ ] 롱폴 40건 동시 중 `/healthz` 100ms 이내
- [ ] `cargo test`, `cargo clippy`, `cargo audit` 통과
- [ ] README / docs / SECURITY.md 정정 완료
