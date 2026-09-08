#!/usr/bin/env bash
#
# One-time SonarQube provisioning, driven entirely through the Web API so the
# instance's configuration is reproducible rather than clicked.
#
#   ./sonarqube/bootstrap.sh
#
# Idempotent: every step tolerates the object already existing, so it is safe to
# re-run after a container restart or a partial failure.
#
# What it does, in order:
#   1. waits for /api/system/status to report UP
#   2. rotates the default admin password (SonarQube ships admin/admin)
#   3. creates the project
#   4. creates the analysis token and writes it into ../.env for Jenkins
#   5. registers the Jenkins webhook that waitForQualityGate() blocks on
#
# The quality gate itself is NOT configured here - see quality-gate.sh, which is
# run after the first analysis so the thresholds can be set against a measured
# coverage number rather than a guess.

set -euo pipefail

cd "$(dirname "$0")"
ENV_FILE="../.env"

[[ -f "$ENV_FILE" ]] || { echo "FATAL: $ENV_FILE not found. Copy .env.example to .env first." >&2; exit 1; }
# shellcheck disable=SC1090
set -a; source "$ENV_FILE"; set +a

SONAR_URL="${SONAR_URL:-http://localhost:9000}"
PROJECT_KEY="${PROJECT_KEY:-stream-processing}"
PROJECT_NAME="${PROJECT_NAME:-Kafka + Flink Stream Processing}"
TOKEN_NAME="${TOKEN_NAME:-jenkins-ci}"
JENKINS_WEBHOOK_URL="${JENKINS_WEBHOOK_URL:-http://jenkins:8080/sonarqube-webhook/}"

DEFAULT_ADMIN_PASS="admin"
NEW_ADMIN_PASS="${SONAR_ADMIN_PASSWORD:-}"
[[ -n "$NEW_ADMIN_PASS" ]] || { echo "FATAL: set SONAR_ADMIN_PASSWORD in .env" >&2; exit 1; }

say() { printf '\n=== %s\n' "$*"; }

# ---------------------------------------------------------------------------
# 1. Wait for UP
#
# The HTTP port answers long before the compute engine is ready; posting an
# analysis report to a STARTING instance fails in a way that looks like a scanner
# bug, so this waits for the real signal.
# ---------------------------------------------------------------------------
say "waiting for SonarQube at $SONAR_URL to report UP"
for i in $(seq 1 80); do
  status=$(curl -fsS "$SONAR_URL/api/system/status" 2>/dev/null | sed -n 's/.*"status":"\([A-Z]*\)".*/\1/p' || true)
  printf '  attempt %2d: %s\n' "$i" "${status:-no-response}"
  [[ "$status" == "UP" ]] && break
  sleep 5
done
[[ "${status:-}" == "UP" ]] || { echo "FATAL: SonarQube did not come UP. Check: docker compose logs sonarqube" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 2. Admin password
#
# Determine which credential currently works, then rotate if it is still the
# default. Doing it in this order makes the script idempotent: on a second run the
# default no longer works and the rotation is skipped rather than failing.
# ---------------------------------------------------------------------------
say "resolving admin credentials"
AUTH=""
if curl -fsS -u "admin:${NEW_ADMIN_PASS}" "$SONAR_URL/api/authentication/validate" 2>/dev/null | grep -q '"valid":true'; then
  echo "  the configured password already works"
  AUTH="admin:${NEW_ADMIN_PASS}"
elif curl -fsS -u "admin:${DEFAULT_ADMIN_PASS}" "$SONAR_URL/api/authentication/validate" 2>/dev/null | grep -q '"valid":true'; then
  echo "  default password in use - rotating"
  # -f is deliberately omitted: SonarQube answers a rejected password with 400 and a
  # JSON body naming the rule that failed, and curl -f throws the body away. Its
  # password policy requires a special character, which a plain alphanumeric
  # generator will not produce - a failure worth reading rather than guessing at.
  resp=$(curl -sS -u "admin:${DEFAULT_ADMIN_PASS}" -X POST \
    --data-urlencode "login=admin" \
    --data-urlencode "previousPassword=${DEFAULT_ADMIN_PASS}" \
    --data-urlencode "password=${NEW_ADMIN_PASS}" \
    -w '\n%{http_code}' \
    "$SONAR_URL/api/users/change_password")
  code=$(printf '%s' "$resp" | tail -1)
  body=$(printf '%s' "$resp" | sed '$d')
  if [[ "$code" != "204" && "$code" != "200" ]]; then
    echo "FATAL: SonarQube rejected the new admin password (HTTP $code)." >&2
    echo "       $body" >&2
    echo "       Fix SONAR_ADMIN_PASSWORD in .env and re-run." >&2
    exit 1
  fi
  echo "  rotated"
  AUTH="admin:${NEW_ADMIN_PASS}"
else
  echo "FATAL: neither the default nor the configured admin password is accepted." >&2
  echo "       If the password was changed by hand, put it in SONAR_ADMIN_PASSWORD." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# 3. Project
# ---------------------------------------------------------------------------
say "creating project $PROJECT_KEY"
if curl -fsS -u "$AUTH" "$SONAR_URL/api/projects/search?projects=${PROJECT_KEY}" | grep -q "\"key\":\"${PROJECT_KEY}\""; then
  echo "  already exists"
else
  curl -fsS -u "$AUTH" -X POST \
    --data-urlencode "project=${PROJECT_KEY}" \
    --data-urlencode "name=${PROJECT_NAME}" \
    "$SONAR_URL/api/projects/create" >/dev/null
  echo "  created"
fi

# ---------------------------------------------------------------------------
# 4. Analysis token
#
# A token, not the admin password: it is scoped, revocable, and it is what ends up
# in Jenkins' credential store. Tokens cannot be read back after creation, so an
# existing one of the same name is revoked and reissued rather than reused.
# ---------------------------------------------------------------------------
say "issuing analysis token '$TOKEN_NAME'"
if curl -fsS -u "$AUTH" "$SONAR_URL/api/user_tokens/search" | grep -q "\"name\":\"${TOKEN_NAME}\""; then
  echo "  a token of that name exists and cannot be read back - revoking it"
  curl -fsS -u "$AUTH" -X POST --data-urlencode "name=${TOKEN_NAME}" \
    "$SONAR_URL/api/user_tokens/revoke" >/dev/null
fi
TOKEN=$(curl -fsS -u "$AUTH" -X POST \
  --data-urlencode "name=${TOKEN_NAME}" \
  --data-urlencode "type=GLOBAL_ANALYSIS_TOKEN" \
  "$SONAR_URL/api/user_tokens/generate" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[[ -n "$TOKEN" ]] || { echo "FATAL: token generation returned nothing" >&2; exit 1; }
echo "  issued (${#TOKEN} chars)"

say "writing SONAR_TOKEN into $ENV_FILE"
if grep -q '^SONAR_TOKEN=' "$ENV_FILE"; then
  # Delimiter is | because a token can contain / but never |
  sed -i "s|^SONAR_TOKEN=.*|SONAR_TOKEN=${TOKEN}|" "$ENV_FILE"
else
  printf 'SONAR_TOKEN=%s\n' "$TOKEN" >> "$ENV_FILE"
fi
echo "  written - recreate Jenkins to pick it up: docker compose up -d --force-recreate jenkins"

# ---------------------------------------------------------------------------
# 5. Jenkins webhook
#
# waitForQualityGate() does not poll; it parks the build and waits for SonarQube to
# POST the finished analysis here. Without this webhook the pipeline hangs until its
# timeout, which looks like a Jenkins fault and is not one.
# ---------------------------------------------------------------------------
say "registering Jenkins webhook -> $JENKINS_WEBHOOK_URL"
if curl -fsS -u "$AUTH" "$SONAR_URL/api/webhooks/list" | grep -q "$JENKINS_WEBHOOK_URL"; then
  echo "  already registered"
else
  curl -fsS -u "$AUTH" -X POST \
    --data-urlencode "name=Jenkins" \
    --data-urlencode "url=${JENKINS_WEBHOOK_URL}" \
    "$SONAR_URL/api/webhooks/create" >/dev/null
  echo "  registered"
fi

say "done"
cat <<EOF
Project:  $SONAR_URL/dashboard?id=${PROJECT_KEY}
Token:    stored in $ENV_FILE as SONAR_TOKEN
Webhook:  ${JENKINS_WEBHOOK_URL}

Next:
  docker compose up -d --build jenkins
  ./sonarqube/quality-gate.sh        # after the first analysis has run
EOF
