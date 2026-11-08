// k6 주문 부하(설계 4장). 실제 주문 서비스 workloads/order의 POST /orders에 KRW-BTC 시장가 주문을 보내고,
// 보낸 주문과 응답을 JSONL v2로 남긴다(계약 명세 2). judge가 이 기록과 DB를 대조해 판정한다.
//
// 실행(클러스터 Job, 설계 4.8):
//   k6 run --log-format=raw --console-output=/out/orders.jsonl /scripts/orders.js
// 로컬(주문 서비스 localhost:8000):
//   K6_TARGET=local ORDER_API=http://localhost:8000 RUN_ID=r-local-0001 PROFILE=smoke ACCOUNTS=5 OUT_DIR=runs/r-local-0001/k6 \
//   k6 run --log-format=raw --console-output=runs/r-local-0001/k6/orders.jsonl load/k6/orders.js
//
// env(괄호는 기본값): RUN_ID(필수), ORDER_API(필수), PROFILE(smoke: smoke|steady|accept|demo), K6_TARGET(local만),
//   ACCOUNTS(300), USER_BASE(0), RATE(30), DURATION_S, SPIKE_AT_S(60), THRESHOLDS(fault: fault|slo), RUN_SEED(run_id 해시),
//   RESEND_RATIO(0.1), RETRY_MAX(3), BUY_KRW(10000), PRICE_HINT(100000000), OUT_DIR(/out, summary.json 위치)

import exec from 'k6/execution';
import {
  CFG,
  INITIAL_CASH,
  MARKET,
  PLAN,
  SCRIPT_VER,
  SETUP_TIMEOUT,
  scenarioOf,
  thresholdsOf,
} from './lib_config.js';
import { placeIntent, readAccounts } from './lib_orders.js';
import { oneLine, protocolErrors, rec, resendMismatch } from './lib_record.js';

const SCENARIO = 'orders';

export const options = {
  scenarios: { [SCENARIO]: scenarioOf(PLAN, CFG.accounts) },
  thresholds: thresholdsOf(CFG.thresholds),
  setupTimeout: SETUP_TIMEOUT,
  // url 태그를 뺀다. user_id가 든 조회 URL이 시계열을 늘린다(설계 4.4). 요청은 tags.name으로 묶는다
  systemTags: ['proto', 'status', 'method', 'name', 'group', 'check', 'error', 'error_code', 'scenario', 'expected_response'],
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

// 시나리오가 하나이고 ACCOUNTS == preAllocatedVUs == maxVUs다(1 VU = 1 user, 설계 4.1)
function checkScenarios(scenarios) {
  const names = Object.keys(scenarios || {});
  if (names.length !== 1) return `시나리오가 ${names.length}개다(1개여야 한다)`;
  const s = scenarios[names[0]];
  if (s.executor !== 'ramping-arrival-rate') return `executor가 ${s.executor}다`;
  if (s.preAllocatedVUs !== CFG.accounts || s.maxVUs !== CFG.accounts) {
    return `preAllocatedVUs ${s.preAllocatedVUs}, maxVUs ${s.maxVUs}가 ACCOUNTS ${CFG.accounts}와 다르다`;
  }
  return null;
}

const initProblem = checkScenarios(options.scenarios);
if (initProblem !== null) throw new Error(`설정 오류: ${initProblem}`);

export function setup() {
  // 명령줄(--vus, --duration 등)이 시나리오를 바꿨으면 멈춘다
  const p = checkScenarios(exec.test.options.scenarios);
  if (p !== null) exec.test.abort(`setup: ${p}`);
  rec({
    type: 'run_start',
    v: 2,
    run: CFG.runId,
    script_ver: SCRIPT_VER,
    seed: CFG.seed,
    profile: CFG.profile,
    plan: PLAN,
    vus: CFG.accounts,
    symbols: [MARKET],
    initial_cash: INITIAL_CASH,
    conn_reuse: !exec.test.options.noConnectionReuse,
    t: Date.now(),
    cfg: {
      user_base: CFG.userBase,
      rate: CFG.rate,
      resend_ratio: CFG.resendRatio,
      retry_max: CFG.retryMax,
      buy_krw: CFG.buyKrw,
      price_hint: CFG.priceHint,
      thresholds: CFG.thresholds,
      target: CFG.target,
    },
  });
  // threshold 지표가 표본 없이 빠지지 않게 0을 넣는다
  protocolErrors.add(0);
  resendMismatch.add(0);
  return { accounts: readAccounts() };
}

export default function (data) {
  placeIntent(data.accounts);
}

export function teardown() {
  rec({ type: 'run_end', run: CFG.runId, t: Date.now() });
}

export function handleSummary(data) {
  delete data.setup_data; // VU 계좌 상태가 크고 판정에 쓰지 않는다
  return {
    [`${CFG.outDir}/summary.json`]: JSON.stringify(data),
    stdout: oneLine(data) + '\n',
  };
}
