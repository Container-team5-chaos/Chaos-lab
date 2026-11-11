"""judge 예외와 종료 코드(설계 1.4).

CLI는 JudgeError(InputError 포함)를 잡아 exit_code로 끝낸다.
그 밖의 예외는 judge 오류로 종료 코드 1, argparse 오류는 2다.
"""

EXIT_PASS = 0
EXIT_FAIL = 10
EXIT_INCONCLUSIVE = 11
EXIT_INPUT = 12
EXIT_JUDGE_ERROR = 1
# CLI에서 아직 구현하지 않은 명령. 위 코드, argparse 2와 겹치지 않는다
EXIT_NOT_IMPLEMENTED = 3

# verdict 값(Outcome 값과 같은 문자열) → 종료 코드. Outcome은 str이라 그대로 조회된다.
EXIT_BY_VERDICT = {
    "PASS": EXIT_PASS,
    "FAIL": EXIT_FAIL,
    "INCONCLUSIVE": EXIT_INCONCLUSIVE,
}


class JudgeError(Exception):
    """judge 자체 오류. 종료 코드 1(설계 1.4)."""

    exit_code = EXIT_JUDGE_ERROR


class InputError(JudgeError):
    """입력 오류(run.json, 기준 파일, 증거 형식). 종료 코드 12(설계 1.4, 6)."""

    exit_code = EXIT_INPUT
