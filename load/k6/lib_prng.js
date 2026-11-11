// 결정적 난수(설계 4.3). VU마다 mulberry32(seed = RUN_SEED ^ vu).
// 같은 RUN_SEED와 같은 서버 응답이면 VU가 내는 주문 순서가 같다.

// mulberry32: 32비트 상태 하나로 [0, 1) 실수를 낸다.
export function mulberry32(seed) {
  let a = seed | 0;
  return function next() {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// 문자열 → 32비트 부호 없는 정수(FNV-1a). RUN_SEED가 없을 때 run_id에서 시드를 만든다.
export function fnv1a32(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}
