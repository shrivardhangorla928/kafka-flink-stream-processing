#!/usr/bin/env bash
# Deletes everything that bills: workloads, the cluster, and the ECR repos.
# Pass --keep-images to leave the ECR repos (a few cents a month) for the next run.
set -euo pipefail

REGION=ap-south-1
CLUSTER=stream-eks

if aws eks describe-cluster --region "$REGION" --name "$CLUSTER" >/dev/null 2>&1; then
  aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER" >/dev/null
  # deleting the namespace first releases the EBS volumes the PVCs created
  kubectl delete namespace stream --timeout=300s || true
  eksctl delete cluster --region "$REGION" --name "$CLUSTER" --wait
fi

if [ "${1:-}" != "--keep-images" ]; then
  for name in ingest-service alert-service flink-pipeline; do
    aws ecr delete-repository --region "$REGION" --repository-name "stream/$name" --force >/dev/null 2>&1 || true
  done
fi

echo "-- leftovers (all should be empty)"
aws eks list-clusters --region "$REGION" --query clusters
aws ec2 describe-volumes --region "$REGION" --filters Name=status,Values=available,in-use \
  --query 'Volumes[].[VolumeId,Size,State]' --output text
aws ec2 describe-nat-gateways --region "$REGION" --filter Name=state,Values=available \
  --query 'NatGateways[].NatGatewayId' --output text
aws elbv2 describe-load-balancers --region "$REGION" --query 'LoadBalancers[].LoadBalancerName' --output text
aws ec2 describe-addresses --region "$REGION" --query 'Addresses[].PublicIp' --output text
