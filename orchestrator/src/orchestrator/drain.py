"""시나리오 6: 대기열이 장애 중 쌓였다가 회복 후 빠지는지 확인한다.

시나리오 YAML 예시
  drain:
    query: sum(adapter_queue_depth)   # adapter의 pending 주문 수
    timeout_s: 300                    # 장애 해제 후 이 시간 안에 빠져야 한다
    threshold: 0                      # 최소 기준선

평소 수준(level) = max(threshold, 워밍업 중 최대값).
부하가 계속 들어오는 동안에는 정상이어도 pending이 몇 건 있으므로, 0이 아니라 평소 수준을 기준으로 본다.

판정(result에 반영)
- 장애 중 값을 한 번도 못 읽음, 또는 마지막까지 못 읽음 → INCONCLUSIVE(drain_unobserved)
- 장애 중 대기열이 평소 수준을 넘은 적이 없음          → INCONCLUSIVE(queue_not_built: 장애가 효과 없음)
- 기한 안에 평소 수준 이하로 내려옴                    → 통과(판정기 결과 유지)
- 기한 안에 안 내려옴                                  → FAILED(queue_not_drained)
"""

DEFAULTS = {"query": "sum(adapter_queue_depth)", "timeout_s": 300, "threshold": 0}


def settings(scenario: dict) -> dict | None:
    block = (scenario or {}).get("drain")
    if not block:
        return None
    if not isinstance(block, dict):
        raise ValueError("drain은 매핑이어야 한다")
    unknown = set(block) - set(DEFAULTS)
    if unknown:
        raise ValueError(f"drain에 모르는 키: {sorted(unknown)}")
    s = {**DEFAULTS, **block}
    if not isinstance(s["timeout_s"], (int, float)) or s["timeout_s"] <= 0:
        raise ValueError("drain.timeout_s는 0보다 커야 한다")
    return s


def new_record(cfg: dict) -> dict:
    return {
        "query": cfg["query"],
        "threshold": cfg["threshold"],
        "timeout_s": cfg["timeout_s"],
        "baseline_peak": None,  # 워밍업(장애 전) 최대값
        "peak": None,  # 장애 이후 최대값
        "samples": 0,
        "unknown_samples": 0,
    }


def sample(rec: dict, value: float | None, baseline: bool = False) -> None:
    rec["samples"] += 1
    if value is None:
        rec["unknown_samples"] += 1
        return
    rec["last"] = value
    key = "baseline_peak" if baseline else "peak"
    rec[key] = value if rec[key] is None else max(rec[key], value)


def level(rec: dict) -> float:
    """'빠졌다'고 볼 평소 수준."""
    return max(rec["threshold"], rec["baseline_peak"] or 0)


def outcome(rec: dict) -> tuple[str | None, str]:
    """(결과를 낮출 값 또는 None, 이유). None이면 판정기 결과를 그대로 둔다."""
    if rec["peak"] is None:
        return "INCONCLUSIVE", "drain_unobserved"
    if rec["peak"] <= level(rec):
        return "INCONCLUSIVE", "queue_not_built"
    if rec.get("drained"):
        return None, "drained"
    if rec.get("last_unknown"):
        return "INCONCLUSIVE", "drain_unobserved"
    return "FAILED", "queue_not_drained"
