# 로컬 실험 환경

스크립트 하나로 kind 클러스터에 실험에 필요한 기반을 띄운다.

| 구성 요소 | Namespace | 버전 | 용도 |
| --- | --- | --- | --- |
| kind (control-plane 1 + worker 1) | - | 설치된 kind 기본 노드 이미지 | 로컬 클러스터 |
| kube-prometheus-stack | `monitoring` | chart 92.21.1 | Prometheus, Grafana, kube-state-metrics |
| Chaos Mesh | `chaos-mesh` | chart 2.8.7 | PodChaos, NetworkChaos |
| metrics-server | `kube-system` | chart 3.14.0 | HPA가 Pod CPU를 읽음 (`kubectl top`) |
| PostgreSQL | `exchange` | `postgres:16-alpine` | 주문, 잔고, 시세 기록 |
| Redis | `exchange` | `redis:7-alpine` | 현재가 캐시 `{price, ts}` |

## 준비물

- Docker (실행 중이어야 함)
- [kind](https://kind.sigs.k8s.io/), kubectl, helm 3
- 메모리 여유 6GB 이상 권장

### Windows

- Docker Desktop을 켜 두고, Git Bash 또는 WSL2 터미널에서 실행한다. PowerShell과 cmd에서는 실행할 수 없다.
- kind, kubectl, helm은 `winget install Kubernetes.kind Kubernetes.kubectl Helm.Helm`으로 설치할 수 있다.
- 실행 권한이 없다고 나오는 경우 `bash infra/local/up.sh`처럼 bash로 직접 실행한다.

## 사용법

```bash
infra/local/up.sh              # 인프라만 생성, 시세는 stub (여러 번 실행 가능)
infra/local/up.sh --with-apps  # 인프라 + 수집기·주문 서비스·데모 화면, 시세는 실제 수집기
infra/local/down.sh            # 삭제
```

`--with-apps`는 아래를 차례로 배포하고, 끝에 시세가 5초 이내로 갱신되는지 확인한다.

| 순서 | 대상 | 방법 |
| --- | --- | --- |
| 1 | 수집기 (C) | 이미지 빌드 → kind 로드 → `infra/k8s/collector/` 적용 |
| 2 | 주문 서비스 (D) | `workloads/order/deploy-local.sh` 그대로 호출 |
| 3 | 데모 화면 (E) | `web/deploy-local.sh`가 있을 때만 |

| 환경 변수 | 기본값 | 의미 |
| --- | --- | --- |
| `PRICE_STUB` | 인프라만 `1`(수집기가 이미 있으면 `0`), `--with-apps` `0` | `1`이면 시세 stub도 띄움. 수집기 시세가 들어오면 stub은 영구히 멈춤(아래 참고) |
| `ORDER_MODE` | 없음 | `baseline`: 주문 서비스를 개선 전 설정으로 배포. `adapter`: 외부 주문 모드(아래) |
| `MOCK_BROKER_DIR` | `workloads/mock-broker` | `ORDER_MODE=adapter`일 때 실행할 mock-broker `deploy-local.sh`의 폴더 |
| `LITE` | `0` | `1`이면 Prometheus·Grafana·Chaos Mesh·metrics-server를 설치하지 않음. 앱 동작만 확인할 때 |

- 수집기는 업비트에 접속해야 한다. 인터넷이 막힌 곳에서는 `PRICE_STUB=1 infra/local/up.sh --with-apps`로 실행한다.
  이때 수집기 시세가 한 번도 들어오지 않으면 stub이 계속 쓰므로 **수집기 장애 실험은 하지 않는다**(앱 동작 확인용).
- stub은 수집기 시세를 한 번 보면 멈추고, Redis `price-stub:yielded`에 표시를 남긴다. stub Pod가 재시작돼도 이 표시를 보고 다시 쓰지 않으므로 수집기 장애가 stub에 가려지지 않는다.
  표시는 `up.sh`가 수집기 없이 stub을 배포할 때만 지우고, 멈춰 있던 stub을 다시 시작한다.
- B의 판정기는 `ticks` 테이블로 체결가의 신선함을 확인한다. stub만 쓰면 `ticks`가 비어 이 항목이 판정 불가(NOT_EVALUATED)가 된다. 판정까지 확인하려면 `--with-apps`로 실제 수집기를 띄운다.
- 이미지 3개를 빌드하므로 처음 실행은 몇 분 더 걸린다. 앱 코드를 바꾼 뒤 다시 실행하면 새 이미지로 재시작한다.

### 외부 주문 모드 (`ORDER_MODE=adapter`)

```bash
ORDER_MODE=adapter infra/local/up.sh --with-apps
```

- mock-broker(C) → adapter(C) → 주문 서비스(`values-adapter.yaml`) 순서로 배포한다. 주문은 `202`(pending)로 접수되고 adapter가 mock-broker에 전달한 뒤 `filled`로 정산된다.
- mock-broker의 `deploy-local.sh`가 없으면 **시작 전에 중단**한다. mock-broker 없이 외부 모드를 켜면 주문이 모두 `unknown`이 되어 예약 금액이 묶이고 `reset-db.sh`도 막힌다.
- 외부 모드에서 즉시 체결 모드로 돌아갈 때(`ORDER_MODE` 없이 다시 실행), 확정되지 않은(pending/unknown) 외부 주문이 있으면 중단한다. 즉시 체결 모드에는 외부 주문을 정산할 worker가 없기 때문이다.
- 데모 화면은 접수(202)를 "체결 대기"로 보여 주고, 주문 조회로 체결·거절을 확인한다. 대기 중 예약 금액은 "체결 대기 예약"에 표시된다.

### 자원이 부족할 때 (LITE)

```bash
LITE=1 infra/local/up.sh --with-apps      # 앱만 띄우기
LITE=1 infra/local/check-apps.sh          # 띄우고 확인까지
```

- 가장 무거운 Prometheus·Grafana(약 2GB)와 Chaos Mesh를 설치하지 않는다. 주문 서비스 차트가 쓰는 ServiceMonitor CRD만 등록한다(Pod 없음).
- 장애 주입, 지표 수집, HPA 자동 확장은 되지 않는다. 화면·주문·수집기 동작 확인용이다.
- 나중에 `LITE` 없이 다시 실행하면 빠진 구성 요소가 추가 설치된다.

### 앱 검증 (`check-apps.sh`)

`up.sh --with-apps`를 실행하고 Pod, 시세·`ticks`, 화면(nginx)·`/api`·`/ws`, 주문·재전송, 시세 끊김 시 `stale_price`, smoke-test를 차례로 확인한다.
결과는 `~/chaos-test/<시각>/result.txt`. 이미 띄운 클러스터에서 확인만 할 때는 `SKIP_UP=1`.

- `up.sh`: 끝에 접속 주소와 port-forward 명령 출력
- 모든 kubectl/helm 호출은 `kind-chaos-lab` context를 명시하므로 다른 클러스터를 건드리지 않음
- 클러스터 이름을 바꾸려면 `CLUSTER_NAME=my-lab infra/local/up.sh`

## 접속

수집기 코드와 배포 절차는 [workloads/collector](../../workloads/collector/README.md)에 있다.
`up.sh --with-apps`가 같은 절차로 배포한다. 수집기만 따로 다시 배포할 때는 README의 명령을 쓴다.
수집기는 기존 `exchange` namespace, `postgres` Secret, PostgreSQL/Redis 서비스를 재사용한다.

| 대상 | 클러스터 내부 주소 | 내 PC에서 열기 |
| --- | --- | --- |
| PostgreSQL | `postgres.exchange.svc.cluster.local:5432` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/postgres 5432` |
| Redis | `redis.exchange.svc.cluster.local:6379` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/redis 6379` |
| Prometheus | `kps-kube-prometheus-stack-prometheus.monitoring.svc:9090` | `kubectl --context kind-chaos-lab -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090` |
| Grafana | - | `kubectl --context kind-chaos-lab -n monitoring port-forward svc/kps-grafana 3000:80` (admin / admin) |
| Chaos Dashboard | - | `kubectl --context kind-chaos-lab -n chaos-mesh port-forward svc/chaos-dashboard 2333` |
| 주문 API (`--with-apps`) | `order.exchange.svc.cluster.local:8000` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/order 8000` |
| adapter (`ORDER_MODE=adapter`) | `adapter.exchange.svc.cluster.local:8000` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/adapter 8001:8000` |
| 데모 화면 (`--with-apps`) | `web.exchange.svc.cluster.local:80` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/web 8080:80` |

## 다른 역할이 알아야 할 것

**서비스 배포 위치**

거래소 서비스는 `exchange` Namespace에 배포한다. DB 접속 정보는 Secret `postgres`에 있다.

**지표 수집**

Prometheus는 Helm release 라벨이 없는 ServiceMonitor/PodMonitor/PrometheusRule도 수집한다. 수집 주기는 5초.

**k6 결과**

remote write receiver가 켜져 있다.

`K6_PROMETHEUS_RW_SERVER_URL=http://kps-kube-prometheus-stack-prometheus.monitoring.svc:9090/api/v1/write`


**주입 제외 대상**

PostgreSQL, Redis Pod에는 `chaos.exchange/protected: "true"` 라벨이 있다. 주입기는 이 라벨이 붙은 Pod를 대상으로 고르지 않는다.


**로컬 전용 설정**

Grafana `admin/admin`, DB `exchange/exchange`, Chaos Dashboard 인증 없음. 공유 환경에서는 쓰지 않는다.

## 주요 에러

| 증상 | 확인할 것 |
| --- | --- |
| `Docker 데몬이 실행 중이 아닙니다` | Docker Desktop이 켜져 있는지 |
| helm 설치가 `--wait` 시간 초과 | `kubectl --context kind-chaos-lab get pods -A`로 Pending·ImagePullBackOff Pod 확인. Docker Desktop 메모리 할당 늘리기 |
| control-plane 컴포넌트가 계속 재시작 | CPU 부족. Docker CPU를 늘리거나 노드 수를 줄인다(현재가 줄인 버전입니다.) |
| `chaos-daemon` 이 Ready가 안 됨 | values의 `chaosDaemon.runtime: containerd`, `socketPath: /run/containerd/containerd.sock` 확인 |
| HPA `TARGETS`가 `<unknown>` | metrics-server 확인: `kubectl --context kind-chaos-lab top pods -n exchange`. 설치 직후 1분 정도는 지표가 없을 수 있음 |
| `60초 동안 신선한 시세가 없습니다` | `kubectl --context kind-chaos-lab -n exchange logs deploy/collector -c collector`. 업비트 접속이 안 되면 `PRICE_STUB=1`로 다시 실행 |
| `ticks에 최근 30초 시세가 없습니다` 경고 | `kubectl --context kind-chaos-lab -n exchange logs deploy/collector -c collector`로 DB 저장 오류 확인. 실험은 되지만 B의 fresh 판정은 NOT_EVALUATED |
| 처음부터 다시 | `infra/local/down.sh` 후 `infra/local/up.sh` |
