#!/usr/bin/env bash
# Gives Grafana, Prometheus, the Flink UI, Jenkins and SonarQube public addresses,
# open only to your current IP. Classic load balancers, plain HTTP.
# Pass MY_IP=1.2.3.4 to override the detected address. Run again if your IP changes.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
MY_IP="${MY_IP:-$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')}"
echo "allowing $MY_IP only"

sed "s#MY_IP#$MY_IP#g" "$HERE/lb/services.yaml" | kubectl apply -f -

for pair in stream/grafana-public stream/prometheus-public stream/flink-public cicd/jenkins-public cicd/sonarqube-public; do
  ns="${pair%%/*}"; name="${pair##*/}"
  host=""
  for _ in $(seq 1 60); do
    host="$(kubectl -n "$ns" get svc "$name" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true)"
    [ -n "$host" ] && break
    sleep 5
  done
  printf '%-18s http://%s\n' "$name" "${host:-PENDING}"
done
echo "Logins: deploy/aws/.secrets.env (Grafana admin, Jenkins admin, SonarQube admin)"
