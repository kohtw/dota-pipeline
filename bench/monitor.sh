#!/usr/bin/env bash
# Samples the VM every 15 s into $OUT/monitor.csv: host CPU busy %, memory used %, load, free disk,
# and the spark pool (slots, running, waiting). Run from the repo root; stop with kill.
set -u
OUT=${OUT:-$HOME/results}; mkdir -p "$OUT"; CSV="$OUT/monitor.csv"
[ -s "$CSV" ] || echo "ts,cpu_busy_pct,mem_used_pct,load1,disk_free_gb,spark_slots,spark_running,spark_waiting" > "$CSV"
read -r _ u n s i w q sq st _ < /proc/stat; prev_total=$((u+n+s+i+w+q+sq+st)); prev_idle=$((i+w))
pool_sql="select (select slots from slot_pool where pool='spark'),
  count(*) filter (where state='running'), count(*) filter (where state in ('scheduled','queued'))
  from task_instance where pool='spark'"
while true; do
  sleep 15
  read -r _ u n s i w q sq st _ < /proc/stat; total=$((u+n+s+i+w+q+sq+st)); idle=$((i+w))
  cpu=$(( 100 * ( (total-prev_total) - (idle-prev_idle) ) / (total-prev_total) )); prev_total=$total; prev_idle=$idle
  mem=$(awk '/MemTotal/{t=$2}/MemAvailable/{a=$2}END{printf "%d", 100*(t-a)/t}' /proc/meminfo)
  load=$(cut -d' ' -f1 /proc/loadavg)
  disk=$(df -BG --output=avail / | tail -1 | tr -dc 0-9)
  pool=$(docker compose exec -T airflow-db psql -U airflow -d airflow -tA -F, -c "$pool_sql" 2>/dev/null | head -1)
  echo "$(date -u +%FT%TZ),$cpu,$mem,$load,$disk,${pool:-,,}" >> "$CSV"
done
