"""Opt-in test: changes only the explicitly selected local kind cluster, restores replicas.

Run against a disposable/demo cluster. Places one simulated order for user-1000.
"""

import os
import subprocess
import time
from pathlib import Path

import pytest
from playwright.sync_api import expect, sync_playwright

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_CLUSTER_TESTS") != "1" or not os.getenv("WEB_BASE_URL"),
    reason="Requires RUN_CLUSTER_TESTS=1 and WEB_BASE_URL (mutates demo cluster)",
)


def kube(*args):
    context = os.getenv("TEST_KUBE_CONTEXT", "kind-chaos-lab")
    if not context.startswith("kind-"):
        raise ValueError("Recovery test is restricted to a local kind context")
    command = (["wsl", "--exec"] if os.name == "nt" else []) + [
        "kubectl",
        "--context",
        context,
        "-n",
        "exchange",
        *args,
    ]
    return subprocess.check_output(command, text=True, encoding="utf-8", timeout=180)


def test_live_web_order_stale_and_pod_recovery():
    collector_replicas = kube(
        "get", "deployment/collector", "-o", "jsonpath={.spec.replicas}"
    ).strip()
    assert int(collector_replicas) > 0
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 1100})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(os.environ["WEB_BASE_URL"], wait_until="domcontentloaded")
            expect(page.locator("#ws-text")).to_have_text("시세 연결됨", timeout=30000)
            page.locator("#user").fill("1000")
            page.locator("#user").dispatch_event("change")
            expect(page.locator("#krw")).not_to_have_text("-", timeout=15000)
            page.locator("#volume").fill("0.0001")
            with page.expect_response(
                lambda r: r.url.endswith("/api/orders") and r.request.method == "POST"
            ) as result:
                page.locator("#submit").click()
            filled = result.value.json()
            assert filled["status"] == "filled", filled
            expect(page.locator("#volume-held")).not_to_have_text(
                "0 BTC", timeout=15000
            )
            expect(page.locator("#pl")).not_to_have_text("보유 코인 없음")
            expect(page.locator("#pl")).not_to_have_text("-")
            assert "원" in page.locator("#valuation").inner_text()

            # Collector outage must become visible without disconnecting WebSocket.
            try:
                kube("scale", "deployment/collector", "--replicas=0")
                kube(
                    "wait",
                    "--for=delete",
                    "pod",
                    "-l",
                    "app=collector",
                    "--timeout=60s",
                )
                expect(page.locator("#ticker")).to_have_class(
                    "ticker is-stale", timeout=15000
                )
                expect(page.locator("#stale-note")).to_be_visible()
            finally:
                kube(
                    "scale", "deployment/collector", f"--replicas={collector_replicas}"
                )
                kube("rollout", "status", "deployment/collector", "--timeout=120s")
            expect(page.locator("#ticker")).not_to_have_class(
                "ticker is-stale", timeout=30000
            )

            # A rolling push restart must close the old socket and let the browser recover.
            old_connections = (
                page.locator("#log li").filter(has_text="시세 연결됨").count()
            )
            kube("rollout", "restart", "deployment/push-server")
            kube("rollout", "status", "deployment/push-server", "--timeout=120s")
            expect(page.locator("#ws-text")).to_have_text("시세 연결됨", timeout=45000)
            expect(
                page.locator("#log li").filter(has_text="시세 연결됨")
            ).to_have_count(old_connections + 1, timeout=45000)

            # Restart Redis itself: push must reconnect, collector repopulates latest price.
            kube("rollout", "restart", "deployment/redis")
            kube("rollout", "status", "deployment/redis", "--timeout=120s")
            # A still-visible pre-restart price is not evidence of recovery.
            restored_at = int(time.time() * 1000)
            page.wait_for_function(
                "after => state.ts >= after", arg=restored_at, timeout=60000
            )
            expect(page.locator("#ws-text")).to_have_text("시세 연결됨", timeout=60000)
            expect(page.locator("#ticker")).not_to_have_class(
                "ticker is-stale", timeout=30000
            )
            kube("rollout", "status", "deployment/push-server", "--timeout=120s")
            assert not errors
            artifact = os.getenv("WEB_SCREENSHOT")
            if artifact:
                path = Path(artifact).with_name("web-recovered.png")
                path.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(path), full_page=True)
        finally:
            browser.close()
