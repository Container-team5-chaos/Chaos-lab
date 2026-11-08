// 계좌 준비, 주문 생성, 전송·재시도·재요청(설계 4.2–4.4, 계약 명세 1–3·11).
// - 멱등 키는 본문 client_order_id다. Idempotency-Key 같은 헤더는 보내지 않는다(Go net/http POST 자동 재전송 조건).
// - 1 VU = 1 user: VU v는 user-<USER_BASE + v>만 쓴다. 같은 계좌로 겹쳐 보내지 않는다.
// - 수량은 BigInt(1e-8 BTC 단위)로 세고 10진 문자열로 보낸다. 본문 문자열은 한 번 만들어 모든 시도에 쓴다.

import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { BATCH, CFG, KEY_MAX, MARKET, TIMEOUT, USER_MAX } from './lib_config.js';
import { mulberry32 } from './lib_prng.js';
import {
  ackMs,
  attemptOf,
  intents,
  kindOf,
  protocolErrors,
  rec,
  resendMismatch,
  results,
  sysErr,
  toRecord,
} from './lib_record.js';

const SAT = 100000000n; // 1 BTC = 1e8 단위(서비스 volume 소수 8자리)
const ASK_P = 0.4; // 보유가 있으면 이 확률로 ask(설계 4.3)
const ASK_HALF_P = 0.7; // ask면 이 확률로 보유 절반, 나머지는 전량
const SETUP_PROBE = 10; // setup 첫 요청 10건이 모두 status 0이면 abort(NetworkPolicy 의심, 설계 4.1)
const SETUP_TRIES = 3; // setup 계좌 읽기: 404가 아닌 실패는 1s 뒤 다시(우리 설정값)
const HEADERS = { 'Content-Type': 'application/json' };
// 재요청 비교 필드(계약 명세 4 RESEND_DIFF의 의미 필드)
const SAME_FIELDS = ['order_id', 'user_id', 'market', 'side', 'volume', 'status', 'fill_price', 'amount'];

export function pad(n, width) {
  let s = String(n);
  while (s.length < width) s = '0' + s;
  return s;
}

export function userId(n) {
  if (!(Number.isInteger(n) && n >= 1 && n <= USER_MAX)) {
    throw new Error(`user 번호 ${n}가 1..${USER_MAX} 밖이다`);
  }
  return `user-${pad(n, 4)}`;
}

// "0.00010000" → 10000n. 형식이 틀리면 throw
export function parseVol(s) {
  const m = /^([0-9]+)(?:\.([0-9]{1,8}))?$/.exec(String(s));
  if (m === null) throw new Error('수량 형식이 틀리다');
  const frac = (m[2] || '').padEnd(8, '0');
  return BigInt(m[1]) * SAT + BigInt(frac);
}

// 10000n → "0.0001"(지수 표기 없음, 소수 8자리 이하)
export function fmtVol(sat) {
  const whole = sat / SAT;
  const frac = pad(String(sat % SAT), 8).replace(/0+$/, '');
  return frac === '' ? String(whole) : `${whole}.${frac}`;
}

function params(name) {
  return { headers: HEADERS, timeout: TIMEOUT, tags: { name } };
}

function accountUrl(user) {
  return `${CFG.orderApi}/accounts/${user}`;
}

// GET /accounts 응답 → {krw, vol}. KRW-BTC 보유가 없으면 vol "0"
function accountOf(res) {
  const b = JSON.parse(res.body);
  let vol = '0';
  for (const p of b.positions || []) {
    if (p.market === MARKET) vol = String(p.volume);
  }
  parseVol(vol);
  return { krw: String(b.krw), vol };
}

// ---------------------------------------------------------------- setup(설계 4.2)

// VU 계좌마다 krw와 보유(앞 run 보유 포함)를 읽는다. 404면 seed 미적용으로 abort(108)
export function readAccounts() {
  const users = [];
  for (let v = 1; v <= CFG.accounts; v++) users.push(userId(CFG.userBase + v));
  const out = [];
  for (let i = 0; i < users.length; i += BATCH) {
    const chunk = users.slice(i, i + BATCH);
    const resps = http.batch(chunk.map((u) => ['GET', accountUrl(u), null, params('account_read')]));
    if (i === 0) {
      const probe = resps.slice(0, SETUP_PROBE); // BATCH ≥ SETUP_PROBE라 첫 묶음에 첫 요청들이 있다
      if (probe.every((r) => r.status === 0)) {
        exec.test.abort(`setup: 첫 요청 ${probe.length}건이 모두 전송 오류다(NetworkPolicy·주소 확인)`);
      }
    }
    for (let j = 0; j < chunk.length; j++) {
      let res = resps[j];
      for (let k = 1; res.status !== 200 && res.status !== 404 && k < SETUP_TRIES; k++) {
        sleep(1);
        res = http.get(accountUrl(chunk[j]), params('account_read'));
      }
      if (res.status === 404) exec.test.abort(`setup: ${chunk[j]}가 없다(seed 미적용)`);
      if (res.status !== 200) exec.test.abort(`setup: ${chunk[j]} 계좌 읽기 실패(status ${res.status})`);
      let acc;
      try {
        acc = accountOf(res);
      } catch (e) {
        exec.test.abort(`setup: ${chunk[j]} 계좌 응답 형식이 틀리다`);
      }
      out.push({ user: chunk[j], krw: acc.krw, vol: acc.vol });
    }
  }
  return out;
}

// ---------------------------------------------------------------- VU 상태

let vuState = null; // {vu, user, held(BigInt), price, rnd, seq}

function vuInit(accounts) {
  const vu = exec.vu.idInTest;
  if (!(vu >= 1 && vu <= CFG.accounts)) {
    exec.test.abort(`VU ${vu}가 ACCOUNTS ${CFG.accounts}를 넘는다(1 VU = 1 user)`);
  }
  const user = userId(CFG.userBase + vu);
  const a = accounts[vu - 1];
  if (!a || a.user !== user) exec.test.abort(`VU ${vu}의 setup 계좌가 ${user}가 아니다`);
  return {
    vu,
    user,
    held: parseVol(a.vol),
    price: CFG.priceHint,
    rnd: mulberry32((CFG.seed ^ vu) | 0),
    seq: 0,
  };
}

// 보유를 다시 읽는다(422·결과 불명 뒤, 설계 4.3). 실패하면 그대로 둔다
function refresh(s) {
  const res = http.get(accountUrl(s.user), params('account_read'));
  if (res.status !== 200) return;
  try {
    s.held = parseVol(accountOf(res).vol);
  } catch (e) {
    // 형식이 틀리면 앞 값을 쓴다
  }
}

// ---------------------------------------------------------------- 주문 생성(설계 4.3)

function nextOrder(s) {
  const r1 = s.rnd();
  const r2 = s.rnd();
  if (s.held > 0n && r1 < ASK_P) {
    const half = s.held / 2n;
    return { side: 'ask', sat: r2 < ASK_HALF_P && half > 0n ? half : s.held };
  }
  let sat = BigInt(Math.floor((CFG.buyKrw * 1e8) / s.price));
  if (sat < 1n) sat = 1n;
  return { side: 'bid', sat };
}

// ---------------------------------------------------------------- 전송(설계 4.4)

const retryable = (a) => a.st === 0 || a.st >= 500; // 결과 불명 + 503 stale_price·price_unavailable

// 같은 본문·같은 키로 보낸다. 재시도는 1s·2s·4s… 뒤 최대 RETRY_MAX회. 시도는 atts에 이어 붙인다
function sendWithRetry(body, name, atts) {
  const p = params(name);
  for (let retry = 0; ; retry++) {
    const t0 = Date.now();
    const res = http.post(`${CFG.orderApi}/orders`, body, p);
    const a = attemptOf(atts.length + 1, t0, Date.now(), res);
    atts.push(a);
    if (!retryable(a) || retry >= CFG.retryMax) return;
    sleep(Math.pow(2, retry));
  }
}

function firstAck(atts) {
  for (const a of atts) if (kindOf(a) === 'ack') return a;
  return null;
}

// 재요청 기대: 200 replayed=true와 같은 의미 필드(설계 4.4). ACK 뒤 확정 거절도 위반이다
function resendDiffers(orig, resent) {
  const ack = firstAck(resent);
  if (ack === null) return kindOf(resent[resent.length - 1]) === 'reject';
  if (ack.rp !== true) return true;
  return SAME_FIELDS.some((f) => String(ack._obj[f]) !== String(orig._obj[f]));
}

// 200 응답의 fill_price·volume·side로 가격과 보유를 갱신한다(응답에 거래 후 보유가 없다)
function applyFill(s, ack) {
  const o = ack._obj;
  const price = Number(o.fill_price);
  if (price > 0) s.price = price;
  let v;
  try {
    v = parseVol(o.volume);
  } catch (e) {
    refresh(s);
    return;
  }
  if (o.side === 'bid') s.held += v;
  else if (o.side === 'ask') s.held = s.held > v ? s.held - v : 0n;
}

// 의도 하나: intent 줄 → 전송(재시도) → 재요청(일부) → result 줄(설계 4.4, 4.6)
export function placeIntent(accounts) {
  if (vuState === null) vuState = vuInit(accounts);
  const s = vuState;
  s.seq += 1;
  const key = `k6-${CFG.runId}-${pad(s.vu, 4)}-${pad(s.seq, 6)}`;
  if (key.length > KEY_MAX) exec.test.abort(`client_order_id가 ${KEY_MAX}자를 넘는다`);
  const o = nextOrder(s);
  const b = { client_order_id: key, user_id: s.user, market: MARKET, side: o.side, volume: fmtVol(o.sat) };
  const body = JSON.stringify(b);
  rec({ type: 'intent', key, acct: s.user, vu: s.vu, t: Date.now(), body: b });
  intents.add(1);

  const atts = [];
  sendWithRetry(body, 'order_create', atts);
  const first = atts[0];
  sysErr.add(retryable(first));
  if (kindOf(first) === 'ack') ackMs.add(first.ms);
  const ack = firstAck(atts);
  if (ack !== null && ack.rp === false && s.rnd() < CFG.resendRatio) {
    const from = atts.length;
    sendWithRetry(body, 'order_resend', atts);
    if (resendDiffers(ack, atts.slice(from))) resendMismatch.add(1);
  }
  for (const a of atts) if (kindOf(a) === 'protocol') protocolErrors.add(1);
  rec({ type: 'result', key, atts: atts.map(toRecord) });
  results.add(1);

  if (ack !== null) {
    applyFill(s, ack);
    return;
  }
  const last = atts[atts.length - 1];
  if (last.st === 422 || kindOf(last) === 'unknown') refresh(s);
}
