def summary(state: dict) -> str:
    lines = [
        f"실험 {state['exp_id']} ({state.get('scenario_name', '-')})",
        f"결과 phase: {state['phase']}",
    ]
    if state.get("aborted_from"):
        lines.append(f"중단 지점: {state['aborted_from']}")
        lines.append(
            f"정리한 리소스: {', '.join(state.get('cleaned_resources', [])) or '없음'}"
        )
    if "verdict" in state:
        lines.append(f"판정: {'PASS' if state['verdict']['passed'] else 'FAIL'}")
    return "\n".join(lines)
