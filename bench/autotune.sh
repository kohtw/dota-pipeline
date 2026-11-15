#!/usr/bin/env bash
# Adjusts the `spark` pool from bench/monitor.sh's samples, once a minute. Raises it by 2 while jobs
# are waiting and the CPU and memory have room; lowers it by 2 when either runs hot. Waits 3 min
# after each change, so new containers reach full load first. Use it to find the slot count
# (session S1 in the runbook), then stop it and keep the pool fixed for the latency sessions.
# Every decision goes to $OUT/autotune.csv. Run from the repo root; stop with kill.
set -u
OUT=${OUT:-$HOME/results}; CSV="$OUT/monitor.csv"; LOG="$OUT/autotune.csv"
MIN=${MIN_SLOTS:-4}; MAX=${MAX_SLOTS:-24}; COOLDOWN=180; last_change=0
[ -s "$LOG" ] || echo "ts,cpu_avg,mem_avg,waiting,slots_before,slots_after,reason" > "$LOG"
while true; do
  sleep 60
  [ "$(wc -l < "$CSV")" -gt 4 ] || continue
  read -r cpu mem slots waiting < <(tail -4 "$CSV" | awk -F, '{c+=$2; m+=$3; s=$6; w=$8} END {printf "%d %d %d %d\n", c/NR, m/NR, s, w}')
  now=$(date +%s); new=$slots; reason=hold
  if [ $((now - last_change)) -lt $COOLDOWN ]; then reason=cooldown
  elif [ "$cpu" -gt 90 ] || [ "$mem" -gt 85 ]; then new=$(( slots - 2 < MIN ? MIN : slots - 2 )); reason=hot
  elif [ "$waiting" -gt 0 ] && [ "$cpu" -lt 70 ] && [ "$mem" -lt 70 ]; then new=$(( slots + 2 > MAX ? MAX : slots + 2 )); reason=room
  fi
  if [ "$new" != "$slots" ]; then
    docker compose exec -T airflow-scheduler airflow pools set spark "$new" "Parser and Spark clean containers" > /dev/null
    last_change=$now
  fi
  echo "$(date -u +%FT%TZ),$cpu,$mem,$waiting,$slots,$new,$reason" >> "$LOG"
done
