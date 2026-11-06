def summary(state: dict) -> str:
    """터미널 요약. 예: result=PASSED  cleanup=OK"""
    lines = [
        f"실험 {state['run_id']} ({state.get('scenario_name', '-')})",
        (
            f"result={state.get('result', '-')}  phase={state['phase']}  "
            f"cleanup={state.get('cleanup', '-')}"
        ),
    ]
    if state.get("reason"):
        lines.append(f"이유: {state['reason']}")
    if state.get("aborted_from"):
        lines.append(f"중단 지점: {state['aborted_from']}")
    if "cleaned_resources" in state:
        lines.append(
            f"정리한 리소스: {', '.join(state['cleaned_resources']) or '없음'}"
        )
    if "verdict" in state:
        lines.append(f"판정: {state['verdict']['outcome']}")
    return "\n".join(lines)
