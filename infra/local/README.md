# 로컬 실험 환경

스크립트 하나로 kind 클러스터에 실험에 필요한 기반을 띄운다.

| 구성 요소 | Namespace | 버전 | 용도 |
| --- | --- | --- | --- |
| kind (control-plane 1 + worker 1) | - | 설치된 kind 기본 노드 이미지 | 로컬 클러스터 |
| kube-prometheus-stack | `monitoring` | chart 92.21.1 | Prometheus, Grafana, kube-state-metrics |
| Chaos Mesh | `chaos-mesh` | chart 2.8.7 | PodChaos, NetworkChaos |
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
infra/local/up.sh      # 생성 (여러 번 실행 가능)
infra/local/down.sh    # 삭제
```

- `up.sh`: 끝에 접속 주소와 port-forward 명령 출력
- 모든 kubectl/helm 호출은 `kind-chaos-lab` context를 명시하므로 다른 클러스터를 건드리지 않음
- 클러스터 이름을 바꾸려면 `CLUSTER_NAME=my-lab infra/local/up.sh`

## 접속

| 대상 | 클러스터 내부 주소 | 내 PC에서 열기 |
| --- | --- | --- |
| PostgreSQL | `postgres.exchange.svc.cluster.local:5432` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/postgres 5432` |
| Redis | `redis.exchange.svc.cluster.local:6379` | `kubectl --context kind-chaos-lab -n exchange port-forward svc/redis 6379` |
| Prometheus | `kps-kube-prometheus-stack-prometheus.monitoring.svc:9090` | `kubectl --context kind-chaos-lab -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090` |
| Grafana | - | `kubectl --context kind-chaos-lab -n monitoring port-forward svc/kps-grafana 3000:80` (admin / admin) |
| Chaos Dashboard | - | `kubectl --context kind-chaos-lab -n chaos-mesh port-forward svc/chaos-dashboard 2333` |

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
| 처음부터 다시 | `infra/local/down.sh` 후 `infra/local/up.sh` |
