#!/usr/bin/env bash
#
# Creates the project's quality gate and binds it to the project.
#
#   ./sonarqube/quality-gate.sh
#
# Run this AFTER the first analysis has been published, because the overall-coverage
# condition is set as a ratchet against the coverage that was actually measured
# rather than an aspirational round number. A gate that fails on the day it is
# created teaches nothing; a gate pinned at today's value fails the moment coverage
# regresses, which is the behaviour worth having in a pipeline.
#
# Two groups of conditions are set, and they do different jobs:
#
#   new code  - the standard "Sonar way" shape. On a pull request or a branch with a
#               baseline these are the ones that bite, and they are strict (A ratings,
#               80% coverage on new lines) because holding new code to a high standard
#               costs nothing when there is little of it.
#   overall   - a floor under the whole codebase, pinned to the measured value. This is
#               what stops the total sliding while every individual change looks fine.
#
# Idempotent: re-running updates the thresholds in place.

set -euo pipefail

cd "$(dirname "$0")"
ENV_FILE="../.env"

[[ -f "$ENV_FILE" ]] || { echo "FATAL: $ENV_FILE not found." >&2; exit 1; }
# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

SONAR_URL="${SONAR_URL:-http://localhost:9000}"
PROJECT_KEY="${PROJECT_KEY:-stream-processing}"
GATE_NAME="${GATE_NAME:-Stream Processing Gate}"
AUTH="admin:${SONAR_ADMIN_PASSWORD}"

say() { printf '\n=== %s\n' "$*"; }

api() { curl -fsS -u "$AUTH" "$@"; }

# ---------------------------------------------------------------------------
# Read the coverage that was actually measured
# ---------------------------------------------------------------------------
say "reading the measured values for $PROJECT_KEY"
measures=$(api "$SONAR_URL/api/measures/component?component=${PROJECT_KEY}&metricKeys=coverage,duplicated_lines_density")

read_measure() {
  printf '%s' "$measures" | python3 -c "
import sys, json
d = json.load(sys.stdin)['component']['measures']
m = sys.argv[1]
print(next((x['value'] for x in d if x['metric'] == m), ''))
" "$1"
}

measured_cov=$(read_measure coverage)
measured_dup=$(read_measure duplicated_lines_density)

if [[ -z "$measured_cov" ]]; then
  echo "FATAL: no coverage measure on $PROJECT_KEY." >&2
  echo "       Run an analysis first: mvn verify sonar:sonar" >&2
  exit 1
fi
echo "  measured overall coverage:   ${measured_cov}%"
echo "  measured duplication:        ${measured_dup:-unknown}%"

# Both overall conditions are ratchets derived from what the project actually
# measures, in the direction that makes today's build pass:
#   coverage    -> floor to the whole percent BELOW  (a higher bar would fail now)
#   duplication -> ceil to the whole percent ABOVE   (a lower bar would fail now)
#
# The first version of this script hard-coded duplication at 5%. The project measures
# 7.5%, so that gate would have failed on the day it was created - which is the exact
# failure this file's header warns about. Deriving the number is the fix; picking a
# rounder-sounding one is not.
floor=$(python3 -c "import math,sys; print(int(math.floor(float(sys.argv[1]))))" "$measured_cov")
COVERAGE_FLOOR="${COVERAGE_FLOOR:-$floor}"

if [[ -n "$measured_dup" ]]; then
  ceil=$(python3 -c "import math,sys; print(int(math.ceil(float(sys.argv[1]))))" "$measured_dup")
else
  ceil=10
fi
DUPLICATION_CEILING="${DUPLICATION_CEILING:-$ceil}"

echo "  -> overall coverage condition:    >= ${COVERAGE_FLOOR}%"
echo "  -> overall duplication condition: <= ${DUPLICATION_CEILING}%"

# ---------------------------------------------------------------------------
# Create (or find) the gate
# ---------------------------------------------------------------------------
say "creating quality gate '$GATE_NAME'"
if api "$SONAR_URL/api/qualitygates/list" | grep -q "\"name\":\"${GATE_NAME}\""; then
  echo "  already exists"
else
  api -X POST --data-urlencode "name=${GATE_NAME}" "$SONAR_URL/api/qualitygates/create" >/dev/null
  echo "  created"
fi

# ---------------------------------------------------------------------------
# Conditions
#
# set_condition() adds a condition, or updates it if that metric is already on the
# gate - SonarQube rejects a duplicate metric outright, so the update path is what
# makes re-running this script safe.
# ---------------------------------------------------------------------------
existing=$(api "$SONAR_URL/api/qualitygates/show?name=$(printf '%s' "$GATE_NAME" | sed 's/ /%20/g')")

set_condition() {
  local metric="$1" op="$2" err="$3"
  local id
  id=$(printf '%s' "$existing" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
m = sys.argv[1]
print(next((c['id'] for c in d.get('conditions', []) if c['metric'] == m), ''))
" "$metric" 2>/dev/null || true)

  if [[ -n "$id" ]]; then
    api -X POST \
      --data-urlencode "id=${id}" \
      --data-urlencode "metric=${metric}" \
      --data-urlencode "op=${op}" \
      --data-urlencode "error=${err}" \
      "$SONAR_URL/api/qualitygates/update_condition" >/dev/null
    printf '  updated %-34s %s %s\n' "$metric" "$op" "$err"
  else
    api -X POST \
      --data-urlencode "gateName=${GATE_NAME}" \
      --data-urlencode "metric=${metric}" \
      --data-urlencode "op=${op}" \
      --data-urlencode "error=${err}" \
      "$SONAR_URL/api/qualitygates/create_condition" >/dev/null
    printf '  added   %-34s %s %s\n' "$metric" "$op" "$err"
  fi
}

say "conditions on new code"
# Ratings are 1=A .. 5=E, so "GT 1" means "worse than A fails".
set_condition new_reliability_rating       GT 1
set_condition new_security_rating          GT 1
set_condition new_maintainability_rating   GT 1
set_condition new_coverage                 LT 80
set_condition new_duplicated_lines_density GT 3

say "conditions on overall code"
set_condition coverage                     LT "${COVERAGE_FLOOR}"
set_condition duplicated_lines_density     GT "${DUPLICATION_CEILING}"

# ---------------------------------------------------------------------------
# Bind the gate to the project
# ---------------------------------------------------------------------------
say "binding '$GATE_NAME' to $PROJECT_KEY"
api -X POST \
  --data-urlencode "gateName=${GATE_NAME}" \
  --data-urlencode "projectKey=${PROJECT_KEY}" \
  "$SONAR_URL/api/qualitygates/select" >/dev/null
echo "  bound"

say "resulting gate"
api "$SONAR_URL/api/qualitygates/show?name=$(printf '%s' "$GATE_NAME" | sed 's/ /%20/g')" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  {d['name']}  (conditions: {len(d.get('conditions', []))})\")
for c in d.get('conditions', []):
    print(f\"    {c['metric']:<34} {c['op']:<3} {c['error']}\")
"

say "current gate status for the project"
api "$SONAR_URL/api/qualitygates/project_status?projectKey=${PROJECT_KEY}" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)['projectStatus']
print('  status:', d['status'])
for c in d.get('conditions', []):
    print(f\"    {c['status']:<6} {c['metricKey']:<34} actual={c.get('actualValue','-'):<10} {c['comparator']} {c.get('errorThreshold','-')}\")
"

cat <<EOF

Gate is live. The next Jenkins build will block on it.
Dashboard: $SONAR_URL/dashboard?id=${PROJECT_KEY}

Note on the new-code conditions: with no baseline set, "new code" is empty and those
conditions pass trivially. Set a baseline (Project Settings > New Code) once there is
a released version to compare against, and they start doing work.
EOF
