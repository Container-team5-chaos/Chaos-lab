"""관측 감시 (시나리오 5-a). 판단할 수 없으면 계속 진행하지 않는다(fail-closed).

워크북 기준: 쿼리 5초 타임아웃, 정상 응답이 없는 구간 상한 15초.
"""

import urllib.request
from typing import Protocol

PROM_URL = "http://kps-kube-prometheus-stack-prometheus.monitoring.svc:9090"


class Watch(Protocol):
    def healthy(self) -> bool: ...


class AlwaysHealthy:
    def healthy(self) -> bool:
        return True


class PrometheusWatch:
    """Prometheus /-/ready 가 200이면 정상. 예외·타임아웃은 비정상으로 본다."""

    def __init__(self, url: str = PROM_URL, timeout: float = 5.0):
        self.url = url.rstrip("/") + "/-/ready"
        self.timeout = timeout

    def healthy(self) -> bool:
        try:
            with urllib.request.urlopen(self.url, timeout=self.timeout) as r:
                return r.status == 200
        except Exception:  # noqa: BLE001 - 어떤 실패든 '관측 불가'
            return False


class ObservabilityLost(Exception):
    """관측이 상한 시간 이상 끊겼다."""
