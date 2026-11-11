// 주문별 기록 JSONL v2와 지표(설계 4.6, 4.7, 계약 명세 2·3).
// 실행 인자 `--log-format=raw --console-output=<OUT_DIR>/orders.jsonl`이면 console.log 한 번이 한 줄이다.
// 이 스크립트에서 console은 기록 줄에만 쓴다. 다른 글을 섞으면 judge 파서가 그 줄을 형식 오류로 센다.

import { Counter, Rate, Trend } from 'k6/metrics';
import { RAW_MAX } from './lib_config.js';

// 4.7 지표. order_intents·order_results는 judge가 줄 수와 대조한다(완전성)
export const intents = new Counter('order_intents');
export const results = new Counter('order_results');
export const sysErr = new Rate('order_sys_err'); // 첫 시도가 전송 오류(0)·5xx
export const ackMs = new Trend('order_ack_ms', true); // 첫 시도가 ACK일 때 그 ms
export const protocolErrors = new Counter('order_protocol_errors');
export const resendMismatch = new Counter('resend_mismatch');

export function rec(obj) {
  console.log(JSON.stringify(obj));
}

function parseObj(text) {
  if (typeof text !== 'string' || text === '') return null;
  try {
    const v = JSON.parse(text);
    return v !== null && typeof v === 'object' && !Array.isArray(v) ? v : null;
  } catch (e) {
    return null;
  }
}

function clip(text) {
  if (text.length <= RAW_MAX) return text;
  let s = text.slice(0, RAW_MAX);
  const last = s.charCodeAt(s.length - 1);
  if (last >= 0xd800 && last <= 0xdbff) s = s.slice(0, -1); // 서로게이트 쌍을 자르지 않는다
  return s;
}

// http 응답 하나 → 시도 {n, t0, t1, st, ec, ms, rp, raw}(계약 명세 2). 8개 키를 모두 쓴다.
// st: 전송 오류·타임아웃이면 0. ec: 거절 본문 reason, 없으면 k6 error_code(문자열), 200은 null.
// ms: k6 timings.duration. rp: 200 본문의 replayed(그 밖 null). raw: 200·4xx·503 본문(2048자 이하).
export function attemptOf(n, t0, t1, res) {
  const st = res.status | 0;
  const text = typeof res.body === 'string' ? res.body : null;
  const obj = parseObj(text);
  let ec = null;
  let rp = null;
  if (st === 200) {
    if (obj !== null && typeof obj.replayed === 'boolean') rp = obj.replayed;
  } else if (st !== 0 && obj !== null && typeof obj.reason === 'string') {
    ec = obj.reason;
  } else if (res.error_code) {
    ec = String(res.error_code);
  }
  const keepRaw = st === 200 || (st >= 400 && st < 500) || st === 503;
  const dur = res.timings && res.timings.duration;
  return {
    n,
    t0,
    t1: Math.max(t0, t1),
    st,
    ec,
    ms: typeof dur === 'number' && dur >= 0 ? dur : 0,
    rp,
    raw: keepRaw && text !== null ? clip(text) : null,
    _obj: st === 200 ? obj : null, // 기록하지 않는다(toRecord가 뺀다)
  };
}

export function toRecord(a) {
  return { n: a.n, t0: a.t0, t1: a.t1, st: a.st, ec: a.ec, ms: a.ms, rp: a.rp, raw: a.raw };
}

// 시도 성격(계약 명세 3, judge model.attempt_kind와 같은 규칙)
const REJECT_STATUSES = [400, 404, 409, 422];
const REJECT_503 = ['price_unavailable', 'stale_price'];

export function kindOf(a) {
  if (a.st === 200) return a._obj !== null && a._obj.order_id != null ? 'ack' : 'protocol';
  if (REJECT_STATUSES.indexOf(a.st) >= 0) return 'reject';
  if (a.st === 503 && REJECT_503.indexOf(a.ec) >= 0) return 'reject';
  if (a.st === 0 || (a.st >= 500 && a.st <= 599)) return 'unknown';
  return 'protocol';
}

// handleSummary stdout 한 줄
export function oneLine(data) {
  const m = data.metrics || {};
  const count = (k) => (m[k] && m[k].values ? m[k].values.count : 0);
  const rate = (k) => (m[k] && m[k].values ? m[k].values.rate : null);
  const p95 = m.order_ack_ms && m.order_ack_ms.values ? m.order_ack_ms.values['p(95)'] : null;
  const fmt = (v, d) => (typeof v === 'number' ? v.toFixed(d) : 'n/a');
  return (
    `orders: intents=${count('order_intents')} results=${count('order_results')} ` +
    `http_reqs=${count('http_reqs')} sys_err=${fmt(rate('order_sys_err'), 4)} ` +
    `ack_p95_ms=${fmt(p95, 1)} protocol=${count('order_protocol_errors')} ` +
    `resend_mismatch=${count('resend_mismatch')} dropped=${count('dropped_iterations')}`
  );
}
