#!/usr/bin/env bash
# Smoke test against the EKS cluster through kubectl port-forward.
set -euo pipefail

NS=stream
station="SMOKE-$(date +%s)"
kubectl -n $NS port-forward svc/ingest-service 18091:80 >/dev/null 2>&1 &
kubectl -n $NS port-forward svc/alert-service 18092:80 >/dev/null 2>&1 &
kubectl -n $NS port-forward svc/flink-jobmanager 18081:8081 >/dev/null 2>&1 &
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
sleep 5

pg() { kubectl -n $NS exec postgres-0 -- psql -U stream -d stream -tAc "$1" 2>/dev/null | tr -d '[:space:]'; }
# reads JSON on stdin and exits non-zero when the python expression is false
check() { python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if ($1) else 1)"; }

echo "-- health"
curl -fsS localhost:18091/actuator/health | check "d['status']=='UP'"
curl -fsS localhost:18092/actuator/health | check "d['status']=='UP'"
echo "ingest and alert UP"

echo "-- flink job"
curl -fsS localhost:18081/jobs/overview | check "any(j['state']=='RUNNING' for j in d['jobs'])"
echo "job RUNNING"

echo "-- topics"
kubectl -n $NS exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | sort

before=$(pg 'SELECT count(*) FROM station_window_aggregates;')
echo "-- posting 250 mm for $station"
curl -fsS -X POST localhost:18091/api/v1/telemetry/readings -H 'Content-Type: application/json' \
  -d '{"stationId":"'"$station"'","sensorType":"RAINFALL","value":250.0}'
echo
deadline=$(( $(date +%s) + 180 ))
until curl -fsS "localhost:18092/api/v1/alerts?stationId=$station" \
      | check "len(d['content'])>0 and d['content'][0]['severity']=='EXTREME'" 2>/dev/null; do
  [ "$(date +%s)" -lt "$deadline" ] || { echo "FAIL: no EXTREME alert after 180s"; exit 1; }
  sleep 5
done
echo "PASS: EXTREME alert for $station"

after=$(pg 'SELECT count(*) FROM station_window_aggregates;')
echo "aggregates: ${before:-0} -> ${after:-0}"
dlq=$(kubectl -n $NS exec kafka-0 -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 \
  --topic telemetry.dlq.v1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
echo "dlq depth: $dlq"
