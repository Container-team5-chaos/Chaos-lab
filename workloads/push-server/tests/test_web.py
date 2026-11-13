"""Read-only acceptance test against the actual deployed web/nginx/push/collector."""

import os
from pathlib import Path

import pytest
from playwright.sync_api import expect, sync_playwright

pytestmark = pytest.mark.skipif(
    not os.getenv("WEB_BASE_URL"), reason="Set WEB_BASE_URL for deployed web"
)


def test_web_live_price_chart_account_and_reconnection():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 960})
            errors = []
            prices = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.add_init_script("""
                window.testSockets = [];
                const NativeWebSocket = window.WebSocket;
                window.WebSocket = class extends NativeWebSocket {
                    constructor(...args) { super(...args); window.testSockets.push(this); }
                };
            """)

            def frame(payload):
                import json

                message = json.loads(payload)
                if message.get("type") == "price":
                    prices.append(message)

            page.on("websocket", lambda ws: ws.on("framereceived", frame))
            page.goto(os.environ["WEB_BASE_URL"], wait_until="domcontentloaded")
            expect(page.locator("#price")).not_to_have_text("-", timeout=30000)
            expect(page.locator("#ws-text")).to_have_text("시세 연결됨", timeout=30000)
            expect(page.locator("#chart-empty")).to_be_hidden(timeout=30000)
            expect(page.locator("#krw")).not_to_have_text("-", timeout=15000)
            assert len(prices) >= 2
            assert all(
                isinstance(m["price"], str) and isinstance(m["ts"], int) for m in prices
            )
            before = len(prices)
            # Close a real browser WebSocket; the app must reconnect through nginx itself.
            page.evaluate("window.testSockets[window.testSockets.length - 1].close()")
            page.wait_for_function("window.testSockets.length >= 2", timeout=20000)
            expect(page.locator("#ws-text")).to_have_text("시세 연결됨", timeout=20000)
            assert len(prices) > before
            assert prices[-1]["ts"] >= prices[before - 1]["ts"]
            assert not errors
            artifact = os.getenv("WEB_SCREENSHOT")
            if artifact:
                Path(artifact).parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=artifact, full_page=True)
        finally:
            browser.close()
