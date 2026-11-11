#!/bin/sh
# k6 Job의 holder 컨테이너(설계 4.8). /out/RELEASE가 생기거나 HOLD_S초가 지날 때까지 Pod를 붙잡아 둔다.
# k6 컨테이너가 OOM 등으로 죽어도 judge fetch가 `kubectl exec -c holder`로 emptyDir /out의 기록을 가져간다.
# judge는 DB 수집 뒤에 `touch /out/RELEASE`를 한다. 그 전에 Job을 지우면 기록이 사라진다.
set -eu
trap 'exit 0' TERM INT

: "${HOLD_S:?HOLD_S가 없다}"
case "$HOLD_S" in
'' | *[!0-9]*)
  echo "hold.sh: HOLD_S는 0 이상 정수여야 한다" >&2
  exit 2
  ;;
esac

end=$(($(date +%s) + HOLD_S))
while [ ! -e /out/RELEASE ] && [ "$(date +%s)" -lt "$end" ]; do
  sleep 2
done
