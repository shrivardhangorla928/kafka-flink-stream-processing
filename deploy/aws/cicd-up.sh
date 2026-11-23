#!/usr/bin/env bash
# Puts SonarQube and Jenkins on the cluster, on the same nodes as the stream stack.
# Run after up.sh creates the cluster. Safe to run again.
set -euo pipefail

REGION=ap-south-1
CLUSTER=stream-eks
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SECRETS="$HERE/.secrets.env"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
JDIR="$ROOT/deploy/cicd/jenkins"
TAG="c-$(cat "$JDIR/Dockerfile" "$JDIR/plugins.txt" "$JDIR/jenkins.yaml" | sha256sum | cut -c1-8)"

[ -f "$SECRETS" ] || { echo "missing $SECRETS, run up.sh first"; exit 1; }
set -a; . "$SECRETS"; set +a

echo "== 1/5 Jenkins image ($TAG)"
aws ecr describe-repositories --region "$REGION" --repository-names stream/jenkins >/dev/null 2>&1 \
  || aws ecr create-repository --region "$REGION" --repository-name stream/jenkins >/dev/null
if aws ecr describe-images --region "$REGION" --repository-name stream/jenkins --image-ids imageTag="$TAG" >/dev/null 2>&1; then
  echo "stream/jenkins:$TAG already in ECR"
else
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
  docker build -t "$REGISTRY/stream/jenkins:$TAG" "$JDIR"
  docker push "$REGISTRY/stream/jenkins:$TAG"
fi

echo "== 2/5 Namespace, IAM role for Jenkins, database secret"
kubectl apply -f "$HERE/cicd/namespace.yaml"
if ! kubectl -n cicd get serviceaccount jenkins >/dev/null 2>&1; then
  eksctl create iamserviceaccount --region "$REGION" --cluster "$CLUSTER" --namespace cicd --name jenkins \
    --attach-policy-arn arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPowerUser --approve
fi
kubectl -n cicd create secret generic sonar-db --from-literal=password="$SONAR_DB_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== 3/5 SonarQube"
kubectl apply -f "$HERE/cicd/sonarqube.yaml"
kubectl -n cicd rollout status statefulset/sonar-db --timeout=300s
kubectl -n cicd rollout status statefulset/sonarqube --timeout=900s
kubectl -n cicd port-forward svc/sonarqube 19000:9000 >/dev/null 2>&1 &
trap 'kill $(jobs -p) 2>/dev/null || true' EXIT
sleep 5
ENV_FILE="$SECRETS" SONAR_URL=http://localhost:19000 \
  JENKINS_WEBHOOK_URL=http://jenkins.cicd.svc.cluster.local:8080/sonarqube-webhook/ \
  "$ROOT/deploy/cicd/sonarqube/bootstrap.sh"
set -a; . "$SECRETS"; set +a

echo "== 4/5 Jenkins"
kubectl -n cicd create secret generic jenkins-env \
  --from-literal=JENKINS_ADMIN_ID="$JENKINS_ADMIN_ID" \
  --from-literal=JENKINS_ADMIN_PASSWORD="$JENKINS_ADMIN_PASSWORD" \
  --from-literal=SONAR_TOKEN="$SONAR_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
sed -e "s#ECR_REGISTRY#$REGISTRY#g" -e "s#IMAGE_TAG#$TAG#g" "$HERE/cicd/jenkins.yaml" | kubectl apply -f -
kubectl -n cicd rollout restart statefulset/jenkins >/dev/null 2>&1 || true
kubectl -n cicd rollout status statefulset/jenkins --timeout=900s

echo "== 5/5 Done"
kubectl -n cicd get pods -o wide
echo "Passwords are in deploy/aws/.secrets.env"
