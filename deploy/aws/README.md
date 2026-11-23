# Deploying on AWS EKS

A short-lived demo cluster in ap-south-1. Two `t3.large` nodes, no NAT gateway, no load balancer.
It is a test cluster for the dissertation, not a production setup.

## What you need

- `aws` (configured with `aws configure`), `eksctl`, `kubectl`, Docker with buildx
- An IAM user that can create EKS, EC2, IAM roles, CloudFormation stacks and ECR repos

## Commands

| Script | What it does |
|---|---|
| `up.sh` | Creates the ECR repos, builds and pushes the three images, creates the cluster, applies `overlays/eks`, waits for every workload. Takes about 25 minutes. Safe to run again. |
| `verify.sh` | Port-forwards the services, checks health and the Flink job, posts a 250 mm reading and waits for the EXTREME alert. |
| `down.sh` | Deletes the namespace, the cluster and the ECR repos, then lists anything left that could bill. `--keep-images` keeps the repos. |

The images are tagged `eks-<git short sha>`. A rerun on the same commit skips the build.

## Reaching the services

Nothing is exposed to the internet. Use port-forward:

```
kubectl -n stream port-forward svc/grafana 3000:3000
kubectl -n stream port-forward svc/prometheus 9090:9090
kubectl -n stream port-forward svc/flink-jobmanager 8081:8081
```

## The eks overlay

`deploy/k8s/overlays/eks` sits on top of the `aws` overlay and changes four things:

- Images come from ECR (`up.sh` fills in the registry and tag).
- A `gp3` default StorageClass on the EBS CSI driver. This EKS version has no default class, so every PVC stayed Pending.
- `flink-state` is ReadWriteOnce, and the TaskManager is pinned to the JobManager's node. EBS cannot be ReadWriteMany.
- Kafka writes to `/var/lib/kafka/data/logs`. An ext4 volume has `lost+found` at its root and Kafka refuses to start with it.

Two fixes went into the `aws` overlay itself, because it had never been run before:

- JobManager memory 600m to 768m (limit 1Gi). At 600m the heap was 24 MB and it ran out of memory.
- TaskManager memory 700m to 1024m (limit 1280Mi). At 700m Flink rejects the memory sizes.
- `PARALLELISM=1` on the JobManager. The job reads its own parallelism and ignored `parallelism.default`, so it asked for 3 slots on a 1-slot cluster.

## Cost

About 25 INR per hour for the whole cluster (control plane, two nodes, disks, public IPs), at 88 INR per USD. The control plane cannot be stopped. Delete the cluster when you are not using it.
