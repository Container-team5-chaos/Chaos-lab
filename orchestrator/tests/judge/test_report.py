"""report.md 조각(설계 5.8, 9 시연 최소선, 계약 정렬 명세 4·7 이름)."""

from decimal import Decimal

from orchestrator.judge import reasons as R
from orchestrator.judge.model import CheckResult, Outcome
from orchestrator.judge.report import REMEDY, fragment, summary_line
from orchestrator.judge.verdict import build_verdict

PASS, FAIL, INC = Outcome.PASS, Outcome.FAIL, Outcome.INCONCLUSIVE

CTX = {"run_id": "r1126-demo", "experiment": "demo-min"}
CRIT_SHA = "c" * 64
RUN_SHA = "a" * 64
EVID_SHA = "e" * 64

CLEAN_INVARIANTS = {
    "cash": 0,
    "qty": 0,
    "avg": 0,
    "nonneg": 0,
    "arith": 0,
    "constraints": 0,
    "fresh": 0,
}


def checks(*extra):
    return [
        CheckResult(id="lost", kind="integrity", outcome=PASS, value=0),
        CheckResult(
            id="order_p95_k6", kind="client", outcome=PASS, value=Decimal("41.5")
        ),
        *extra,
    ]


def guards(*extra):
    return [
        CheckResult(
            id="safety", kind="guard", outcome=PASS, on_violation="inconclusive"
        ),
        *extra,
    ]


def verdict(**kw):
    args = dict(
        ctx_like=CTX,
        checks=checks(),
        guards=guards(),
        integrity={
            "intents": {"total": 27000, "ack": 26990},
            "log_complete": True,
            "classes": {"OK": 26990, "UNRESOLVED_ABSENT": 10},
            "subs": {
                "clean": 26980,
                "after_commit": 7,
                "not_committed": 2,
                "after_503": 0,
                "rejected": 1,
            },
            "invariants": dict(CLEAN_INVARIANTS),
        },
        client={
            "phases": {
                "tail": {"n": 3600, "ack_p95_ms": 44.25, "sys_err_ratio": 0.0},
                "baseline": {
                    "n": 4500,
                    "ack_p95_ms": Decimal("41.5"),
                    "sys_err_ratio": 0.0012,
                    "achieved_ratio": 0.999,
                },
            }
        },
        load={},
        inputs={
            "criteria_sha256": CRIT_SHA,
            "run_json_sha256": RUN_SHA,
            "evidence_manifest_sha256": EVID_SHA,
            "judge_version": "0.1.0",
            "response_fields_confirmed": False,
        },
        warnings=(),
        run_info={"duration_ms": 1},
        generated_at=1_764_000_000_000,
    )
    args.update(kw)
    return build_verdict(**args)


def test_first_line_is_demo_summary():
    text = fragment(verdict())
    assert text.splitlines()[0] == (
        "유실 0 · 불일치 0 · 중복 0 · 고아 0 · 잔고 불일치 0 · 신선도 위반 체결 0 → PASS"
    )
    assert summary_line(verdict()) == text.splitlines()[0]


def test_counts_follow_term_mapping():
    """유실·중복·불일치·고아는 설계 5.3 완료 기준 용어 대응을 따른다."""
    v = verdict(
        integrity={
            "intents": {"total": 10, "ack": 9},
            "classes": {
                "LOST": 1,
                "REJECT_COMMITTED": 2,
                "MISMATCH": 2,
                "DUP_ORDER": 1,
                "IDEMPOTENCY_VIOLATION": 3,
                "ORPHAN": 4,
                "INTERRUPTED": 5,
            },
            "invariants": {**CLEAN_INVARIANTS, "cash": 1, "avg": 2, "fresh": 3},
        },
        checks=checks(CheckResult(id="dup", kind="integrity", outcome=FAIL, value=4)),
    )
    first = fragment(v).splitlines()[0]
    assert first == (
        "유실 3 · 불일치 2 · 중복 4 · 고아 4 · 잔고 불일치 3 · 신선도 위반 체결 3 → FAIL"
    )


def test_not_evaluated_classes_are_not_shown_as_counts():
    """평가하지 않은 클래스(integrity.not_evaluated)는 확정 값처럼 쓰지 않는다(설계 5.3, 5.7)."""
    v = verdict(
        integrity={
            "classes": {"OK": 8, "REJECT_COMMITTED": 1, "ORPHAN": 1},
            "invariants": dict(CLEAN_INVARIANTS),
            "not_evaluated": {
                "LOST": R.ACCOUNT_SET_MISMATCH,
                "ORPHAN": R.SENT_LOG_INCOMPLETE,
                "UNRESOLVED_ABSENT": R.SENT_LOG_INCOMPLETE,
            },
        },
    )
    text = fragment(v)
    first = text.splitlines()[0]
    assert first.startswith(
        "유실 ≥1(ACCOUNT_SET_MISMATCH) · 불일치 0 · 중복 0 · 고아 ?(SENT_LOG_INCOMPLETE)"
    )
    assert "| 고아(ORPHAN) | ?(SENT_LOG_INCOMPLETE) |" in text
    assert (
        "| 결과 불명·중단(사용자 영향: UNRESOLVED_COMMITTED+UNRESOLVED_ABSENT+INTERRUPTED)"
        " | ?(SENT_LOG_INCOMPLETE) |"
    ) in text
    star = verdict(
        integrity={"classes": None, "not_evaluated": {"*": R.DB_ERROR}},
    )
    assert summary_line(star).startswith("유실 ?(DB_ERROR) · 불일치 ?(DB_ERROR)")


def test_unknown_values_are_question_marks():
    v = verdict(integrity={"classes": {"OK": 1}, "invariants": {"cash": 0}})
    first = fragment(v).splitlines()[0]
    assert "잔고 불일치 ?" in first and "신선도 위반 체결 ?" in first
    assert first.startswith("유실 0 · 불일치 0")


def test_stale_fill_falls_back_to_check_value():
    inv = {k: v for k, v in CLEAN_INVARIANTS.items() if k != "fresh"}
    v = verdict(
        integrity={"classes": {}, "invariants": inv},
        checks=checks(
            CheckResult(id="stale_fill", kind="integrity", outcome=FAIL, value=2)
        ),
    )
    assert "신선도 위반 체결 2 → FAIL" in fragment(v).splitlines()[0]


def test_core_sections_present():
    text = fragment(verdict())
    for phrase in (
        "커밋 후 회수 7건(after_commit)",
        "## 판정: PASS",
        "| 의도(intent) | 27000 |",
        "| ACK(200, order_id) | 26990 |",
        "| 유실(LOST+REJECT_COMMITTED) | 0 |",
        "| 내용 불일치(MISMATCH+BODY_MISMATCH) | 0 |",
        "| 중복(DUP_ORDER+FILL_STATE+IDEMPOTENCY_VIOLATION) | 0 |",
        "잔고 불일치(cash 0, qty 0, avg 0)",
        "신선도 위반 체결(fresh)",
        "그 밖 불변식: arith 0, constraints 0, nonneg 0",
        "| not_committed | 2 |",
        "| rejected | 1 |",
        "| baseline | 4500 | 41.5 | 0.0012 | 0.999 |",
        "| tail | 3600 | 44.25 | 0 | — |",
        f"- criteria_sha256: `{CRIT_SHA}`",
        f"- run_json_sha256: `{RUN_SHA}`",
        f"- evidence_manifest_sha256: `{EVID_SHA}`",
        "- response_fields_confirmed: `아니오`",
        "| 기준 | `lost` | integrity | PASS |",
    ):
        assert phrase in text, phrase
    assert f"- verdict_digest: `{verdict().verdict_digest}`" in text
    # 구간은 설계 순서(baseline이 tail보다 먼저)
    assert text.index("| baseline |") < text.index("| tail |")
    assert "INCONCLUSIVE 사유와 해소 방법" not in text


def test_inconclusive_reasons_have_remedies():
    v = verdict(
        checks=checks(
            CheckResult(
                id="invariants",
                kind="integrity",
                outcome=INC,
                reasons=(R.SCHEMA_DRIFT,),
            ),
            CheckResult(
                id="protocol",
                kind="unresolved",
                outcome=FAIL,
                on_violation="inconclusive",
            ),
        ),
        guards=guards(
            CheckResult(
                id="fill_coverage",
                kind="guard",
                outcome=FAIL,
                on_violation="inconclusive",
            )
        ),
    )
    assert v.verdict is INC and v.reasons == (R.LOW_FILL_COVERAGE,)
    text = fragment(v)
    assert "### INCONCLUSIVE 사유와 해소 방법" in text
    assert f"| `{R.SCHEMA_DRIFT}` | `invariants` | {REMEDY[R.SCHEMA_DRIFT]} |" in text
    assert f"| `{R.LOW_FILL_COVERAGE}` | `fill_coverage` |" in text
    assert f"| `{R.CHECK_INCONCLUSIVE}` | `protocol` |" in text
    assert "schema.sql" in text
    assert (
        "| 가드 | `fill_coverage` | guard | FAIL→INCONCLUSIVE | 예 | — "
        f"| `{R.LOW_FILL_COVERAGE}` |"
    ) in text


def test_money_fail_note_and_safety_warning_on_top():
    v = verdict(
        checks=checks(
            CheckResult(id="orphans", kind="integrity", outcome=FAIL, value=1)
        ),
        guards=guards(
            CheckResult(
                id="account_set",
                kind="guard",
                outcome=INC,
                reasons=(R.SAFETY_VIOLATION,),
            )
        ),
    )
    lines = fragment(v).splitlines()
    assert lines[0].endswith("→ FAIL")
    assert R.SAFETY_VIOLATION in lines[3]  # 요약 두 줄 바로 뒤
    assert "- 돈 불변식 FAIL은 가드보다 앞선다(설계 5.7 규칙 1)." in lines


def test_after_503_warning_shown():
    integ = {
        "classes": {"OK": 3},
        "subs": {"after_503": 2},
        "invariants": CLEAN_INVARIANTS,
    }
    text = fragment(verdict(integrity=integ))
    assert f"after_503 2건은 경고 `{R.ROLLBACK_REPLY_COMMITTED}`" in text
    assert f"- 경고: `{R.ROLLBACK_REPLY_COMMITTED}`" in text


def test_fragment_is_deterministic_and_ignores_generated_at():
    a = fragment(verdict())
    b = fragment(verdict(generated_at=1_999_999_999_999, run_info={"x": 2}))
    assert a == b == fragment(verdict())


def test_table_cells_are_escaped():
    v = verdict(
        checks=checks(CheckResult(id="x", kind="client", outcome=PASS, value="a|b")),
    )
    assert "a\\|b" in fragment(v)


def test_remedy_covers_contract_reasons_only():
    """옛 계약 사유는 없다. 새 사유(SCHEMA_DRIFT, SNAPSHOT_LARGE)는 해소 방법이 있다(명세 6, 7)."""
    assert {R.SCHEMA_DRIFT, R.SNAPSHOT_LARGE, R.JUDGE_SELF_CHECK} <= set(REMEDY)
    assert set(REMEDY) <= R.ALL_REASONS
    assert not any("pt_ro" in t or "REPLAY" in t for t in REMEDY.values())
