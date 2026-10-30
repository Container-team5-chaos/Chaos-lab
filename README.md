# chaos-lab

상태 인지형 카오스 테스트 도구

## 구조

```
infra/          terraform
platform/       helm values (chaos-mesh, prometheus, keda, karpenter)
workloads/      피실험 시스템 (vllm, tei, open-webui)
experiments/    시나리오, 평가 기준
orchestrator/   실험 실행기
load/           k6
docs/           adr, 다이어그램
```
