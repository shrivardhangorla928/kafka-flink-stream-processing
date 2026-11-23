#!/usr/bin/env bash
# Builds the images, creates the EKS cluster and deploys the stream stack.
# Safe to run again: existing repos, images and the cluster are reused.
set -euo pipefail

REGION=ap-south-1
CLUSTER=stream-eks
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TAG="${TAG:-eks-$(git -C "$ROOT" rev-parse --short HEAD)}"
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"

echo "== 1/4 ECR repos and images ($TAG)"
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
for pair in ingest:ingest-service alert:alert-service flinkjob:flink-pipeline; do
  target="${pair%%:*}"; name="${pair##*:}"
  aws ecr describe-repositories --region "$REGION" --repository-names "stream/$name" >/dev/null 2>&1 \
    || aws ecr create-repository --region "$REGION" --repository-name "stream/$name" >/dev/null
  if aws ecr describe-images --region "$REGION" --repository-name "stream/$name" --image-ids imageTag="$TAG" >/dev/null 2>&1; then
    echo "stream/$name:$TAG already in ECR"
  else
    docker buildx build --platform linux/amd64 -f "$ROOT/deploy/docker/Dockerfile" --target "$target" \
      -t "$REGISTRY/stream/$name:$TAG" --push "$ROOT"
  fi
done

echo "== 2/4 EKS cluster"
if aws eks describe-cluster --region "$REGION" --name "$CLUSTER" >/dev/null 2>&1; then
  echo "cluster $CLUSTER exists"
  aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER" >/dev/null
else
  eksctl create cluster -f "$ROOT/deploy/aws/cluster.yaml"
fi
kubectl get nodes

echo "== 3/4 Deploy"
kubectl kustomize "$ROOT/deploy/k8s/overlays/eks" \
  | sed -e "s#ECR_REGISTRY#$REGISTRY#g" -e "s#IMAGE_TAG#$TAG#g" \
  | kubectl apply -f -

echo "== 4/4 Wait for workloads"
kubectl -n stream wait --for=condition=complete job --all --timeout=300s || true
for w in statefulset/kafka statefulset/postgres deploy/flink-jobmanager deploy/flink-taskmanager \
         deploy/ingest-service deploy/alert-service deploy/prometheus deploy/grafana; do
  kubectl -n stream rollout status "$w" --timeout=400s
done
kubectl -n stream get pods -o wide
echo "Next: deploy/aws/verify.sh"
