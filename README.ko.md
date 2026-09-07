# Photon relay

[English](README.md)

ChibiGeo를 포함한 Photon 호환 지오코딩용 작은 Go 라운드로빈 중계기입니다.
별도 DB 서버, Redis, 지도 데이터셋 없이 Linux 컨테이너 한 개로 실행합니다.

## 기능

- GET /api, /reverse 요청을 사용 가능한 제공자 사이에서 라운드로빈 처리.
- 제공자별 최소 요청 간격(최소 1초), UTC 일간·월간 한도.
- 실패한 시도도 포함하여 외부 요청 전에 쿼터를 영속 저장.
- 전체 4.3초 안에 최대 네 제공자까지 대체 시도, 오류 및 Retry-After에 따른 대기.
- 파일로 마운트한 키를 해당 제공자에만 X-Api-Key로 전달.
- /healthz와 Prometheus /metrics. 좌표·키·응답 본문은 로그에 기록하지 않음.

## 제공자 선택 (0.2.2)

최상위 `"selection_mode": "round_robin"`(생략 시 기본값) 또는
`"selection_mode": "recent_data"`를 설정합니다. 알 수 없는 모드는 시작을 거부합니다.
라운드로빈은 사용 가능한 제공자를 순환합니다. 최신 데이터 모드는 확인된 Photon
`import_date`가 최신인 제공자를 우선하고 동일 날짜끼리는 순환합니다.
날짜 미상·메타데이터 비활성 제공자는 후순위입니다.

```json
{"selection_mode":"recent_data","recent_data":{"granularity":"month"}}
```

`recent_data.granularity`는 `year`(연도), `month`(기본값, 연·월),
`day`(연·월·일), `time`(전체 시각)을 지원합니다. 같은 구간끼리는 순환합니다.
예를 들어 month에서 8월 8일과 8월 29일은 동순위지만 1월과 8월은 다릅니다.
서버 시간대가 아닌 UTC로 구분하며 잘못된 값이면 시작을 거부합니다.
`photon_relay_recent_data_granularity_info`로 설정값을 노출하며
라운드로빈 모드에서는 정밀도 설정이 선택에 영향을 주지 않습니다.

확인 실패 시 마지막으로
확인된 날짜를 유지하며 실패 표시는 별도로 남습니다. 최초 확인 전 날짜가 모두
미상이면 일반 순환합니다. 포인트·작업 시각이 아닌 제공자 지도 데이터 최신성입니다.
오래된 제공자도 대체 후보로 남으며 최대 데이터 나이 제한은 없습니다.

모든 모드는 요청 간격·쿨다운·이미 시도한 제공자 제외·영속 쿼터·기존 제한된 대체
시도를 공유합니다. 최신 제공자가 한도 소진·간격 제한 중이면 기다리지 않고
다음 후보를 선택합니다. 설정된 한도 내에서 최신 제공자에 요청이 집중될 수 있습니다.
추가 모드를 위해 순위 결정을 `candidateOrder`로 분리했으며 현재 지원은 위 두 가지입니다.
설정 변경 후 기존 상태 볼륨을 유지한 단일 레플리카를 재시작합니다.
상태 마이그레이션이나 쿼터 초기화는 필요 없습니다.
`photon_relay_selection_mode_info{mode="…"} 1`로 현재 모드를 확인합니다.

## 설정

providers.example.json을 providers.json으로 복사하고 예시 Photon 주소를
사용이 허용된 서버로 바꾸세요. 이 예시는 공개 서버 목록이 아닙니다.
ChibiGeo 주소에는 /v1/photon이 포함되며 key_file에는 API 키만 저장합니다.
ChibiGeo도 동등한 라운드로빈 대상이며, 실패 시에만 사용하는 대상은 아닙니다.

interval_ms는 요청 시작 사이 최소 간격입니다. daily/monthly는 자체 한도이며
0은 해당 한도 비활성화입니다. 예시 수치는 제공자의 사용량 보장이 아닙니다.
실제 플랜과 운영 정책을 확인하세요. 일간·월간 집계는 UTC 날짜·월 시작에 초기화됩니다.

주의: 이 중계기를 거친 요청만 집계합니다. 키의 기존 사용량이나 다른 사용처가 있다면
활성화 전에 운영 예산에서 그 사용량을 제외해야 합니다.
계정·IP 교체로 제공자의 제한을 우회하지 마세요.

## 실행

docker build -f Containerfile -t photon-relay:local . 로 빌드합니다.
providers.json은 /config/providers.json에 읽기 전용으로, 키는
/run/secrets/chibigeo에 읽기 전용으로, 영속 쓰기 공간은 /data에 마운트합니다.
UID/GID 10001로 실행하므로 키 읽기와 저장소 쓰기 권한이 필요합니다.

CONFIG_FILE 기본값은 /config/providers.json, STATE_FILE은 /data/quota.json입니다.
8080 포트를 사용합니다. 자체 수신 인증이 없으므로 반드시 신뢰하는 내부망에서
앱·모니터링 클라이언트만 접근하도록 제한하고 인터넷에 직접 공개하지 마세요.

Dawarich는 보호된 내부망에서 PHOTON_API_HOST=photon-relay:8080,
PHOTON_API_USE_HTTPS=false로 연결합니다. 제공자 키는 클라이언트가 아닌 중계기에 둡니다.
실제 배포의 DNS·Secret·매니페스트는 이 저장소에 넣지 않습니다.

## 운영 제약

쿼터 파일은 하나의 프로세스·레플리카만 소유합니다. 프로세스 잠금으로 같은 경로의
중복 실행을 막습니다. 업데이트 시 /data를 보존하세요. 삭제하면 집계가 초기화됩니다.
쿼터 파일 손상·쓰기 실패 시 외부 요청을 중단합니다. 분산 쿼터, 응답 캐시,
자동 리큐잉, 제공자 자동 검색은 구현하지 않았습니다.
외부 시도는 최대 2.5초, 전체 시도는 4.3초 안으로 제한하며 가능한 경우
마지막 대체 시도용 800ms를 남깁니다. 먼 서버는 여전히 시간 초과될 수 있습니다.
전체 제공자가 사용 불가·한도 초과면 가짜 빈 데이터 대신 503을 반환합니다.
정상적인 빈 FeatureCollection은 재시도 없이 그대로 반환합니다.

## 영속 일별 집계와 메트릭

### 제공자 데이터 갱신일 (0.2.1)

공개 제공자마다 `"status_enabled": true`로 켭니다. 백그라운드가 `/status`를
최대 24시간에 한 번(매시간 스케줄 검사) 확인하며 인증정보·리다이렉트·재시도는
사용하지 않습니다. 확인 시각과 마지막 정상 import_date를 기존 JSON에 보존하며
`/metrics` 자체는 외부 요청을 만들지 않습니다. 상태 확인은 지오코딩 요청 한도·집계와
별도입니다. 지원이 확인된 공개 상태 API가 없는 유료 제공자에서는 끄세요.
확인 실패 시 마지막 날짜는 유지하되 실패 상태를 노출합니다. 최신 데이터 모드는
보존된 날짜를 사용하고 라운드로빈은 날짜를 무시합니다. 어느 모드도 오래된
데이터라는 이유만으로 제공자를 제외하지 않습니다.

메트릭: `photon_relay_upstream_data_timestamp_seconds`(모르면 없음),
`photon_relay_upstream_metadata_enabled`, `photon_relay_upstream_metadata_success`,
`photon_relay_upstream_metadata_checked_timestamp_seconds`,
`photon_relay_upstream_metadata_last_success_timestamp_seconds`.
데이터 나이는 `time() - photon_relay_upstream_data_timestamp_seconds`이며,
확인 성공 여부·확인 시각의 경과도 따로 보세요. 날짜 문자열을 라벨로 만들지 않습니다.

별도 DB나 SQLite 대신 영속 저장소의 작은 JSON 파일을 사용합니다.
제공자마다 스키마 버전 1, UTC 일·월 사용량, 이전 집계 기준량, 누적 시도·결과,
지연시간 버킷, 최근 사용 기록이 있는 35일의 집계를 저장합니다.
요청이 없는 동안에도 일·월 표시값은 UTC 경계에 초기화되며 다음 요청 승인 때
이전 날짜를 기록합니다. 시계가 뒤로 가도 한도를 다시 열지 않습니다.
프로세스 잠금, 임시 파일 쓰기, fsync, rename, 디렉터리 fsync를 사용하고
저장 실패 시 새 외부 요청을 중단합니다.

버전 0 상태는 한도를 지우지 않고 이전합니다. 원본은 STATE_FILE.pre-v1에
한 번 보존한 뒤 새 상태를 저장합니다. 기존 사용량은 외부 소비량·첫날의 보수적
예약을 포함할 수 있으므로 새로 관측한 트래픽으로 표시하지 않습니다.
두 파일과 PVC를 보존하세요. 이 체크포인트는 외부 백업이 아닙니다.
같은 계정 한도를 소비하는 중에 오래된 쿼터를 복원하지 마세요.
0.1.x로 내리면 다음 쓰기에서 새 일별 기록·결과 필드가 사라지므로 다운그레이드하지 마세요.

메트릭은 HELP/TYPE 선언과 누적 히스토그램을 포함한 Prometheus text 0.0.4이며
CI에서 promtool로 실제 엔드포인트를 검사합니다. 제공자 시리즈 라벨은 제공자명과
고정 결과 분류뿐이며 URL·좌표·API 키·사용자 라벨은 포함하지 않습니다.

| 메트릭 (photon_relay_ 접두사) | 의미 |
|---|---|
| daily_quota_used / monthly_quota_used | 기준 예약량을 포함한 사용 예산 |
| daily_quota_baseline / monthly_quota_baseline | 이전 집계·외부 예약량이며 관측 요청 수가 아님 |
| daily_requests / monthly_requests | 기준량을 제외한 현재 기간의 신규 승인 시도 |
| attempts_total | 스키마 이전 이후의 영속 누적 승인 시도 |
| success_total / failure_total / outcomes_total | 영속 관측 결과와 고정 실패 분류 |
| upstream_request_duration_seconds | 실패 지연도 포함한 영속 히스토그램 |
| daily_remaining / monthly_remaining | 남은 로컬 예산; -1은 로컬 상한 없음 |
| quota_reset_timestamp_seconds | 다음 UTC 일·월 경계; UTC 자정은 KST 09:00 |
| eligible / next_eligible_timestamp_seconds / cooldown_until_seconds | 현재 사용 가능 여부와 대기; 가동률 보장은 아님 |
| last_success_timestamp_seconds / last_failure_timestamp_seconds / last_http_status | 마지막 관측 결과; 시각·상태 0은 없음 |
| quota_storage_healthy / quota_state_write_errors_total / quota_state_size_bytes | 저장 상태·쓰기 오류·파일 크기 |
| client_requests_total / inflight_requests / process_start_time_seconds | 클라이언트 응답·동시 요청·프로세스 시작; probe 제외 |

시도 승인은 전송 전에 저장합니다. 그 사이 프로세스가 죽으면 실제 전송보다
많이 집계할 수 있고, 결과 저장 전에 죽으면 마지막 결과가 누락될 수 있습니다.
외부 API와 exactly-once를 보장하지 않습니다. 제공자 카운터·히스토그램은
재시작 후 보존되며 클라이언트 응답·쓰기 오류 카운터는 프로세스마다 새로 시작합니다.
일·월 요청 메트릭은 gauge이므로 속도 계산에는 attempts_total을 사용하세요.
0.1의 daily_requests는 기준 예약량을 포함했지만 0.2부터 명확히 분리합니다.

PromQL 예시(환경에 맞게 job/namespace 필터 추가):

```promql
sum by (provider) (rate(photon_relay_attempts_total[5m]))
sum by (provider, outcome) (increase(photon_relay_outcomes_total[1h]))
histogram_quantile(0.95, sum by (provider, le) (rate(photon_relay_upstream_request_duration_seconds_bucket[15m])))
photon_relay_daily_remaining
```

[Prometheus 출력 규칙](https://prometheus.io/docs/instrumenting/exposition_formats/)을 참고하세요.

## 개발·릴리스 명령

go test -race ./...
go vet ./...

CI는 테스트·이미지 빌드 후 제공자에 요청하지 않고 /healthz를 확인합니다.
vMAJOR.MINOR.PATCH 태그는 검증된 linux/amd64 이미지를
ghcr.io/kimwonj77/photon-relay에 버전·Git 리비전 라벨과 함께 발행합니다.
CI는 클러스터 인증정보나 배포 명령을 사용하지 않습니다.
공개 Git 레포만으로 새 GHCR 패키지가 공개되지는 않으므로 패키지 공개 여부를 별도 확인해야 합니다.

[GitHub 컨테이너 레지스트리 문서](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry),
[ChibiGeo API 문서](https://chibigeo.com/docs/api-doc/).
