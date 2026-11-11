// 설정과 안전장치(설계 4.1, 4.5, 계약 명세 2·11).
// init에서 읽고 검사한다. 어기면 throw → k6 종료 코드 107 → judge INCONCLUSIVE(LOAD_INVALID).
// 계약 근거: workloads/order app/config.py(SUPPORTED_MARKETS), app/schemas.py(client_order_id ≤ 64),
// db/seed.sql(user-0001..user-1000, 각 10,000,000원).

import { fnv1a32 } from './lib_prng.js';

export const SCRIPT_VER = 'orders-0.1.0';
export const MARKET = 'KRW-BTC'; // 서비스 SUPPORTED_MARKETS. 이 종목만 보낸다
export const USER_MAX = 1000; // seed 계좌 user-0001..user-1000
export const INITIAL_CASH = 10000000; // seed 계좌 시작 잔고(run_start 참고용)
export const KEY_MAX = 64; // 서비스 client_order_id 상한
export const RAW_MAX = 2048; // attempt.raw 상한(계약 명세 2)
export const TIMEOUT = '5s'; // 요청 타임아웃(우리 설정값, k6 기본 60s)
export const GRACEFUL_STOP = '60s'; // 의도 하나의 최악 길이 54s보다 길게(설계 4.4)
export const SETUP_TIMEOUT = '180s'; // 계좌 읽기(설계 4.2)
export const BATCH = 20; // setup 계좌 읽기 http.batch 크기
export const TIME_UNIT_S = 60; // ramping-arrival-rate timeUnit '1m'(설계 4.5)
export const SPIKE_S = 10; // accept·demo 스파이크 길이
export const SPIKE_MULT = 10; // 스파이크 도착률 = RATE × 10(기본 30/s → 300/s, L604)

// 주문 서비스 Service(이름 order, 포트 8000)가 있는 namespace. 다르면 여기만 바꾼다(설계 4.1 <ns>)
export const K8S_NAMESPACE = 'exchange';
const NS = K8S_NAMESPACE.replace(/[.]/g, '\\.');
// 클러스터 안 주문 서비스 8000 포트뿐(설계 4.1)
const CLUSTER_API_RE = new RegExp(
  `^http://order(\\.${NS}(\\.svc(\\.cluster\\.local)?)?)?:8000(/.*)?$`,
);
// K6_TARGET=local일 때만: 이 노트북의 로컬 주문 서비스·가짜 서버
const LOCAL_API_RE = /^http:\/\/(127\.0\.0\.1|localhost):[0-9]+(\/.*)?$/;
const RUN_ID_RE = /^[a-z0-9-]{3,40}$/; // run.json run_id(설계 1.4)

const PROFILES = ['smoke', 'steady', 'accept', 'demo'];
const LATER_PROFILES = ['step', 'replay']; // 4.10, 4.11. 2차
const THRESHOLD_SETS = ['fault', 'slo'];

function env(name) {
  const v = __ENV[name];
  return v === undefined || v === '' ? undefined : v;
}

function or(v, def) {
  return v === undefined ? def : v;
}

function bad(msg) {
  throw new Error(`설정 오류: ${msg}`);
}

function intEnv(name, def, lo, hi) {
  const raw = env(name);
  if (raw === undefined) return def;
  if (!/^[0-9]+$/.test(raw)) bad(`${name}는 0 이상 정수여야 한다`);
  const v = Number(raw);
  if (!Number.isSafeInteger(v) || v < lo || v > hi) bad(`${name}=${v}가 ${lo}..${hi} 밖이다`);
  return v;
}

function numEnv(name, def, lo, hi) {
  const raw = env(name);
  if (raw === undefined) return def;
  if (!/^[0-9]+(\.[0-9]+)?$/.test(raw)) bad(`${name}는 0 이상 10진수여야 한다`);
  const v = Number(raw);
  if (!(v >= lo && v <= hi)) bad(`${name}=${v}가 ${lo}..${hi} 밖이다`);
  return v;
}

function oneOf(name, def, allowed) {
  const v = or(env(name), def);
  if (allowed.indexOf(v) < 0) bad(`${name}=${v}는 ${allowed.join('|')} 중 하나여야 한다`);
  return v;
}

// ORDER_API 검사(설계 4.1). 제3자 호스트·공개 ALB는 막는다. 끝의 /는 뗀다
export function checkOrderApi(url, target) {
  if (url === undefined) bad('ORDER_API가 없다');
  const ok = CLUSTER_API_RE.test(url) || (target === 'local' && LOCAL_API_RE.test(url));
  if (!ok) {
    bad(
      `ORDER_API가 허용 주소가 아니다. 클러스터는 http://order.${K8S_NAMESPACE}.svc.cluster.local:8000, ` +
        '로컬은 K6_TARGET=local과 http://localhost:<port>만 된다',
    );
  }
  return url.replace(/\/+$/, '');
}

function readConfig() {
  const runId = env('RUN_ID');
  if (runId === undefined || !RUN_ID_RE.test(runId)) bad('RUN_ID는 ^[a-z0-9-]{3,40}$여야 한다');

  const profile = or(env('PROFILE'), 'smoke');
  if (LATER_PROFILES.indexOf(profile) >= 0) bad(`PROFILE=${profile}는 2차다(설계 4.10, 4.11)`);
  if (PROFILES.indexOf(profile) < 0) bad(`PROFILE=${profile}는 ${PROFILES.join('|')} 중 하나여야 한다`);

  const target = env('K6_TARGET');
  if (target !== undefined && target !== 'local') bad('K6_TARGET은 local이거나 없어야 한다');
  if (env('THRESHOLDS') === 'step') bad('THRESHOLDS=step은 2차다(설계 4.10)');
  const thresholds = oneOf('THRESHOLDS', 'fault', THRESHOLD_SETS);

  const accounts = intEnv('ACCOUNTS', 300, 1, USER_MAX);
  const userBase = intEnv('USER_BASE', 0, 0, USER_MAX - 1);
  if (userBase + accounts > USER_MAX) bad(`USER_BASE + ACCOUNTS = ${userBase + accounts} > ${USER_MAX}(seed 계좌 수)`);

  const seedRaw = env('RUN_SEED');
  const seed = seedRaw === undefined ? fnv1a32(runId) : intEnv('RUN_SEED', 0, 0, 0xffffffff);

  return {
    runId,
    profile,
    target: or(target, 'cluster'),
    orderApi: checkOrderApi(env('ORDER_API'), target),
    accounts,
    userBase,
    rate: numEnv('RATE', 30, 0.1, 1000),
    durationS: intEnv('DURATION_S', undefined, 1, 86400),
    spikeAtS: intEnv('SPIKE_AT_S', 60, 0, 86400),
    thresholds,
    seed,
    resendRatio: numEnv('RESEND_RATIO', 0.1, 0, 1),
    retryMax: intEnv('RETRY_MAX', 3, 0, 10),
    buyKrw: intEnv('BUY_KRW', 10000, 5000, 1000000),
    priceHint: numEnv('PRICE_HINT', 100000000, 1, 1e12),
    outDir: or(env('OUT_DIR'), '/out').replace(/\/+$/, ''),
  };
}

export const CFG = readConfig();

// 프로파일 → 구간 [{rps, s}](설계 4.5)
function segments(c) {
  const spike = c.rate * SPIKE_MULT;
  switch (c.profile) {
    case 'smoke':
      return [{ rps: 1, s: or(c.durationS, 10) }];
    case 'steady':
      return [{ rps: c.rate, s: or(c.durationS, 600) }];
    case 'accept':
      return [
        { rps: c.rate, s: 300 },
        { rps: spike, s: SPIKE_S },
        { rps: c.rate, s: 180 },
      ];
    case 'demo': {
      const total = or(c.durationS, 900);
      const rest = total - c.spikeAtS - SPIKE_S;
      if (rest < 1) bad(`demo: DURATION_S(${total})가 SPIKE_AT_S + ${SPIKE_S}보다 커야 한다`);
      const segs = [];
      if (c.spikeAtS > 0) segs.push({ rps: c.rate, s: c.spikeAtS });
      segs.push({ rps: spike, s: SPIKE_S }, { rps: c.rate, s: rest });
      return segs;
    }
    default:
      return bad(`PROFILE=${c.profile}`);
  }
}

// 구간마다 {0s 점프} + {유지} stage. 같은 값으로 k6 시나리오와 run_start.plan을 만든다
export function buildPlan(c) {
  const segs = segments(c);
  const stages = [];
  for (const g of segs) {
    const target = Math.round(g.rps * TIME_UNIT_S);
    if (target < 1) bad(`도착률 ${g.rps}/s가 너무 작다(분당 1 미만)`);
    stages.push({ duration_s: 0, target }, { duration_s: g.s, target });
  }
  return {
    executor: 'ramping-arrival-rate',
    time_unit_s: TIME_UNIT_S,
    start_rate: stages[0].target,
    stages,
  };
}

export const PLAN = buildPlan(CFG);

export function scenarioOf(plan, vus) {
  return {
    executor: 'ramping-arrival-rate',
    startRate: plan.start_rate,
    timeUnit: `${plan.time_unit_s}s`,
    preAllocatedVUs: vus,
    maxVUs: vus,
    stages: plan.stages.map((s) => ({ duration: `${s.duration_s}s`, target: s.target })),
    gracefulStop: GRACEFUL_STOP,
  };
}

// threshold는 계약 위반 지표와 클러스터 보호에만 쓴다. SLO 판정은 judge가 한다(설계 4.7)
export function thresholdsOf(set) {
  const t = {
    order_protocol_errors: ['count==0'],
    resend_mismatch: ['count==0'],
  };
  if (set === 'slo') {
    t.order_ack_ms = ['p(95)<300']; // 실패(99)는 judge 경고 K6_THRESHOLD_DISAGREES
    t.order_sys_err = ['rate<0.01'];
  }
  return t;
}
