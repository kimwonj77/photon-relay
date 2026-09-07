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

## 개발·릴리스

go test -race ./...
go vet ./...

CI는 테스트·이미지 빌드 후 제공자에 요청하지 않고 /healthz를 확인합니다.
vMAJOR.MINOR.PATCH 태그는 검증된 linux/amd64 이미지를
ghcr.io/kimwonj77/photon-relay에 버전·Git 리비전 라벨과 함께 발행합니다.
CI는 클러스터 인증정보나 배포 명령을 사용하지 않습니다.
공개 Git 레포만으로 새 GHCR 패키지가 공개되지는 않으므로 패키지 공개 여부를 별도 확인해야 합니다.

[GitHub 컨테이너 레지스트리 문서](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry),
[ChibiGeo API 문서](https://chibigeo.com/docs/api-doc/).
