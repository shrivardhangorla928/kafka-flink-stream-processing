# Kubernetes deployment

Kustomize manifests for the stream processing pipeline: Kafka in KRaft mode, PostgreSQL,
a Flink **application-mode** cluster running `TelemetryPipelineJob`, and the two
Spring Boot services.

This is the Kubernetes equivalent of `deploy/docker/docker-compose.yml`. Environment
variable names, ports, health endpoints and Flink properties are deliberately the
same in both, so a value that works in one is the value that works in the other.

```
deploy/k8s/
  base/                          production-shaped: 3 brokers, RF 3, 3 alert replicas
    kustomization.yaml
    namespace.yaml               namespace `stream`, Pod Security `restricted`
    kafka/                       StatefulSet (KRaft combined mode), 2 Services, PDB,
                                 topic-provisioning Job + its ConfigMap
    postgres/                    StatefulSet, Secret, Service
    flink/                       JobManager + TaskManager Deployments, Services,
                                 checkpoint PVC, config/{jobmanager,taskmanager}-config.yaml
    ingest-service/              Deployment, Service, ConfigMap
    alert-service/               Deployment, Service, ConfigMap
  overlays/
    local/                       single-node minikube; 1 broker, RF 1, 2 alert replicas
      flink/                     smaller Flink memory sizes
      patches/
      flink-ui-nodeport.yaml
    staging/                     multi-node; base shape, larger resources, pull Always
      patches/
      pdb.yaml                   disruption budgets for the stateless tiers
```

---

## Prerequisites

| | |
|---|---|
| `kubectl` | v1.36 (Kustomize v5.8.1 is built in - `kubectl kustomize`, `kubectl apply -k`) |
| `minikube` | with the **docker** driver |
| Docker | the daemon minikube runs inside, and the one the images are built into |
| JDK 21 + Maven 3.9+ | only if you build outside the Dockerfile |

Helm is not used and is not required.

---

## Bring-up: local (minikube)

### 1. Start the cluster

```bash
minikube start \
  --driver=docker \
  --nodes=1 \
  --cpus=6 \
  --memory=10g \
  --disk-size=20g \
  --addons=metrics-server
```

Why these numbers - see [Resource arithmetic](#resource-arithmetic) for the full
working:

* **`--nodes=1`.** A second node would buy nothing here and would break one thing:
  the Flink checkpoint PVC is `ReadWriteMany`, which minikube's hostpath provisioner
  only really satisfies while every pod is on the same node.
* **`--cpus=6`.** Pod CPU *requests* total 1.85. Six leaves the workstation's other
  ten cores to the IDE and Jenkins, and gives the JVMs room to burst during start-up.
* **`--memory=10g`.** Pod memory *requests* total 5.00 GiB; the sum of *limits* is
  9.00 GiB. Ten covers the worst case with room for the kubelet and kube-system.
  `--memory=8g` also works in practice - the Kafka heap (`-Xmx512m`) and both Flink
  process sizes (768m / 1024m) are fixed rather than percentage-of-limit, so the real
  steady state is around 6.3 GiB - but a simultaneous burst on both Spring services
  could then push the node into eviction. 10 GiB is the number with no asterisk on it.
* **`--addons=metrics-server`.** Not needed now; phase 3's HPA needs it and enabling
  it at cluster creation is one less thing to remember.

### 2. Build the three images into minikube's Docker daemon

The `local` overlay uses `imagePullPolicy: IfNotPresent` and the `stream/*` images
exist in no registry, so they have to be built inside the daemon the kubelet reads
from. `eval $(minikube docker-env)` repoints your shell's Docker client at it:

```bash
cd <repo root>
eval $(minikube docker-env)

docker build -f deploy/docker/Dockerfile --target ingest   -t stream/ingest-service:1.0.0 .
docker build -f deploy/docker/Dockerfile --target alert    -t stream/alert-service:1.0.0  .
docker build -f deploy/docker/Dockerfile --target flinkjob -t stream/flink-pipeline:1.0.0 .

docker images | grep stream/          # all three must be listed here, not just locally
```

The Dockerfile compiles the whole Maven reactor once in a shared `builder` stage, so
the three builds cost one Maven run, not three.

Run `eval $(minikube docker-env -u)` to point your shell back at the host daemon.

### 3. Apply

```bash
kubectl apply -k deploy/k8s/overlays/local

kubectl -n stream get pods -w
```

### 4. Tear down

```bash
kubectl delete -k deploy/k8s/overlays/local
# PVCs created from volumeClaimTemplates are not deleted with the StatefulSet:
kubectl -n stream delete pvc --all
```

---

## Bring-up: staging

```bash
kubectl apply -k deploy/k8s/overlays/staging
```

Differences from `local`: 3 Kafka brokers with replication factor 3 and
`min.insync.replicas=2`, 3 alert-service replicas, roughly 4x the memory,
`imagePullPolicy: Always`, and PodDisruptionBudgets on ingest-service,
alert-service and the TaskManagers.

It assumes a registry the cluster can pull `stream/*` from, and - see the scope
boundaries - a StorageClass that can genuinely serve `ReadWriteMany`.

---

## Bring-up order, and how the manifests enforce it

`kubectl apply -k` submits every object at once; Kubernetes has no dependency graph.
The ordering below is enforced by the pods themselves, which is the only place it
can be enforced reliably:

| Step | What waits, and how |
|---|---|
| 1. Kafka, PostgreSQL | Nothing depends on anything. Both are StatefulSets with startup probes (TCP 9092 / `pg_isready`) that gate their Service endpoints. |
| 2. `kafka-topic-init` Job | Retries `kafka-topics.sh --list` every 5s for up to 5 minutes before creating anything. All four `--create` calls are `--if-not-exists`, so the Job is safe to re-run. |
| 3. Flink JobManager | An init container blocks on a TCP connect to `kafka:9092`. TaskManagers start in parallel and retry registration with the JobManager on Flink's own schedule. |
| 4. ingest-service | Init container blocks on `kafka:9092`. |
| 5. alert-service | Two init containers: `pg_isready` against `postgres:5432`, then `kafka:9092`. Flyway then runs inside the application at boot, which the 210s startup probe budget covers. |

A pod that is not Ready is not in its Service's endpoint list, so nothing routes to a
service that cannot yet answer, regardless of what order the pods happened to start in.

The topic Job carries `ttlSecondsAfterFinished: 600`. A Job's spec is immutable, so
if you change the replication factor in `kafka-topics` while the completed Job still
exists, delete it before re-applying:

```bash
kubectl -n stream delete job kafka-topic-init --ignore-not-found
```

---

## Reaching things

The `local` overlay publishes three fixed NodePorts. The numbers echo the Compose
host ports so there is one mental model for both stand-ups.

```bash
MINIKUBE_IP=$(minikube ip)
```

| Endpoint | URL | Compose equivalent |
|---|---|---|
| Ingest API | `http://$MINIKUBE_IP:30091` | `http://localhost:8091` |
| Alert API | `http://$MINIKUBE_IP:30092` | `http://localhost:8092` |
| Flink web UI | `http://$MINIKUBE_IP:30081` | `http://localhost:8081` |

`minikube service -n stream ingest-service --url` prints the same thing and opens a
tunnel if the driver needs one.

Anything without a NodePort - Kafka, PostgreSQL, the Prometheus endpoints - is
reached by port-forward:

```bash
kubectl -n stream port-forward svc/kafka                    9092:9092
kubectl -n stream port-forward svc/postgres                 5433:5432
kubectl -n stream port-forward svc/ingest-service           8091:80
kubectl -n stream port-forward svc/alert-service            8092:80
kubectl -n stream port-forward svc/flink-jobmanager         8081:8081
```

Note that port-forwarding Kafka to the host gives you a *bootstrap* connection only:
the broker advertises its in-cluster DNS name, which the host cannot resolve, so a
host-side client will bootstrap and then fail to reach the partition leaders. Run
Kafka CLI tools inside the cluster instead:

```bash
kubectl -n stream exec -it kafka-0 -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
```

---

## Verifying it works

### The topics exist with the right shape

```bash
kubectl -n stream logs job/kafka-topic-init
```

Four topics, 3 partitions each; `retention.ms=86400000` on the two telemetry topics
and `604800000` on `alerts.generated.v1` and `telemetry.dlq.v1`.

### The Flink job is running

```bash
# from outside
curl -s http://$(minikube ip):30081/jobs/overview | jq '.jobs[] | {name, state, start-time}'

# or without a NodePort
kubectl -n stream exec deploy/flink-jobmanager -- \
  curl -s localhost:8081/jobs/overview
```

`state` must be `RUNNING`. In the web UI the job graph should show three parallel
subtasks per operator, and the checkpoint tab a completed checkpoint every 30s.

Three TaskManagers, one slot each, all registered:

```bash
kubectl -n stream exec deploy/flink-jobmanager -- \
  curl -s localhost:8081/overview
# expect "taskmanagers":3, "slots-total":3, "slots-available":0 while the job runs
```

`slots-available: 0` is the healthy state - the parallelism-3 job has taken all three.

### Alerts are landing in PostgreSQL

Force a burst rather than waiting out a 5-minute window plus 30s of watermark lag:

```bash
curl -XPOST "http://$(minikube ip):30091/api/v1/telemetry/simulate/burst?stormStations=10"
```

Then, one window plus the watermark lag later:

```bash
curl -s "http://$(minikube ip):30092/api/v1/alerts?severity=WARNING&size=20" | jq .

kubectl -n stream exec -it postgres-0 -- \
  psql -U stream -d stream -c \
  'SELECT severity, count(*), max(generated_at) FROM alerts GROUP BY severity ORDER BY 1;'

kubectl -n stream exec -it postgres-0 -- \
  psql -U stream -d stream -c 'SELECT count(*) FROM station_window_aggregates;'
```

Nothing in `alerts` but rows in `station_window_aggregates` means the pipeline is
running and no threshold was breached - raise `stormStations`.

### Metrics are exposed (phase 3 depends on this)

```bash
kubectl -n stream exec deploy/ingest-service -- curl -s localhost:8080/actuator/prometheus | head
kubectl -n stream exec deploy/flink-jobmanager -- curl -s localhost:9249/metrics | head
```

Both Spring pods and both Flink pod sets carry `prometheus.io/scrape`,
`prometheus.io/port` and `prometheus.io/path` annotations, so an annotation-driven
Prometheus discovers them with no further configuration. The four Services also carry
`monitoring: stream` as a selector handle for a Prometheus Operator `ServiceMonitor`.
`flink-taskmanager-metrics` is headless on purpose: a load-balanced ClusterIP would
scrape a different one of the three TaskManagers each interval.

---

## Resource arithmetic

### `local` overlay - the budget that has to fit

Target: total pod **requests** at or under **5 GiB memory and 2 CPU**, on a
workstation with 16 cores and about 15 GiB free that is also running an IDE and a
Jenkins container.

| Workload | Replicas | Request each | Requests total | Limit each |
|---|---:|---|---:|---|
| kafka | 1 | 512Mi / 250m | 512Mi / 250m | 1Gi / 1 |
| postgres | 1 | 256Mi / 100m | 256Mi / 100m | 512Mi / 500m |
| flink-jobmanager | 1 | 512Mi / 250m | 512Mi / 250m | 1Gi / 1 |
| flink-taskmanager | 3 | 640Mi / 250m | 1920Mi / 750m | 1280Mi / 1 |
| ingest-service | 2 | 384Mi / 100m | 768Mi / 200m | 640Mi / 500m |
| alert-service | 2 | 448Mi / 100m | 896Mi / 200m | 768Mi / 500m |
| **steady state** | | | **4864Mi / 1750m** | |
| kafka-topic-init (runs once, then TTLs away) | 1 | 256Mi / 100m | 256Mi / 100m | 512Mi / 500m |
| **peak, during bring-up** | | | **5120Mi / 1850m** | |

`5120Mi = 5.00 GiB` and `1850m = 1.85 CPU`. Both inside the budget, with the peak
sitting exactly on the memory ceiling for the few seconds the provisioning Job runs
and 4.75 GiB thereafter.

Two notes on where the slack is:

* Init containers do not add to this. A pod's effective request is
  `max(sum(containers), max(initContainers))`, and every init container here requests
  32-64Mi against app containers requesting 384Mi or more.
* The requests are deliberately below the real steady-state footprint of some of
  these JVMs. Requests are a *scheduling* floor; on a single-node workstation
  overcommitting them is the only way six JVMs fit at all. The **limits** are the
  numbers that actually protect the node, and each one is set above the JVM sizing
  inside it:

| | JVM sizing | Container limit | Headroom |
|---|---|---|---|
| kafka | `-Xmx512m -Xms512m` | 1Gi | 512Mi for page cache and direct buffers |
| flink-jobmanager | `jobmanager.memory.process.size: 768m` | 1Gi | 256Mi |
| flink-taskmanager | `taskmanager.memory.process.size: 1024m` | 1280Mi | 256Mi |
| ingest-service | `-XX:MaxRAMPercentage=75` -> ~480Mi heap | 640Mi | 160Mi non-heap |
| alert-service | `-XX:MaxRAMPercentage=75` -> ~576Mi heap | 768Mi | 192Mi non-heap |

Sum of limits: `1024 + 512 + 1024 + (3 × 1280) + (2 × 640) + (2 × 768) = 9216Mi = 9.00 GiB`.
That is the reason the recommended `--memory` is 10g rather than 8g.

Storage: 2Gi (Kafka) + 2Gi (Postgres) + 1Gi (Flink state) = 5Gi against a 20g disk.

### `staging` overlay

| Workload | Replicas | Request each | Requests total |
|---|---:|---|---:|
| kafka | 3 | 2Gi / 1 | 6Gi / 3 |
| postgres | 1 | 1Gi / 500m | 1Gi / 500m |
| flink-jobmanager | 1 | 2Gi / 500m | 2Gi / 500m |
| flink-taskmanager | 3 | 2Gi / 500m | 6Gi / 1.5 |
| ingest-service | 2 | 1Gi / 250m | 2Gi / 500m |
| alert-service | 3 | 1Gi / 250m | 3Gi / 750m |
| **steady state** | | | **20.00 GiB / 6.75 CPU** |
| kafka-topic-init (transient) | 1 | 256Mi / 100m | 0.25 GiB / 100m |
| **peak, during bring-up** | | | **20.25 GiB / 6.85 CPU** |

Three worker nodes of 4 vCPU / 10 GiB each is the smallest shape that holds this with
enough headroom for the anti-affinity rules to actually spread the brokers: 30 GiB and
12 vCPU of raw capacity against 20.25 GiB and 6.85 CPU of requests leaves roughly a
third of each node free for the kubelet, kube-system and burst.

The Flink pods are memory-`Guaranteed` in staging (requests equal limits): their JVMs
are configured with a fixed process size, so being schedulable into memory they will
never be allowed to use buys nothing, and matching the two keeps them out of the first
tier the kubelet evicts under node pressure.

---

## Design notes

### Kafka: per-broker identity in KRaft

Two settings cannot be one static value across a StatefulSet:

* `node.id` - unique per broker;
* `advertised.listeners` - must be *this pod's own* DNS name. A client bootstraps via
  `kafka:9092` and is then told where each partition leader lives; if all three
  brokers advertised the same name, every client would be bounced back to a random
  broker for every partition.

Both are derived at start-up by an entrypoint wrapper in
`base/kafka/statefulset.yaml`, from the one thing a StatefulSet pod always knows about
itself - its ordinal, which is the suffix of its hostname:

```
node.id                  = ordinal + 1
advertised.listeners     = PLAINTEXT://$HOSTNAME.kafka-headless.$NAMESPACE.svc.cluster.local:9092
controller.quorum.voters = built by looping 0..KAFKA_CLUSTER_SIZE-1 over the same name template
```

The voter list is generated from the same template rather than written out by hand, so
it cannot drift from the advertised names, and changing the cluster size is one
`KAFKA_CLUSTER_SIZE` value in a ConfigMap. The wrapper echoes all three derived values
before `exec`ing the image's real entrypoint, so `kubectl logs kafka-0 | head` shows
exactly what the broker was told.

Two supporting details that this depends on:

* `kafka-headless` sets `publishNotReadyAddresses: true`. A KRaft quorum forms by the
  voters reaching each other, and none of them is Ready until it has. Without this the
  DNS records the voters need would not be published until after they were needed.
* `podManagementPolicy: Parallel`. `OrderedReady` deadlocks a cold KRaft start:
  kafka-0 cannot reach a quorum, so it never becomes Ready, so kafka-1 is never
  created.

### Flink: application mode, and `config.yaml` not `flink-conf.yaml`

`standalone-job` runs the job's `main()` on the JobManager, so one cluster owns exactly
one job and the job's lifecycle *is* the Deployment's lifecycle. A session cluster
needs a separate submission step whose success Kubernetes cannot see, which is how a
"healthy" Flink cluster ends up running no job at all.

**The configuration file is `config.yaml`.** Verified against the image rather than
assumed: `/opt/flink/conf` in `flink:1.20.5-scala_2.12-java17` contains `config.yaml`
and no `flink-conf.yaml`, and `bin/config-parser-utils.sh` - which the image's
entrypoint runs - selects `flink-conf.yaml` only `if [ -e ... ]` and otherwise falls
back to `config.yaml`. The files here are written in the same nested standard-YAML form
the distribution itself ships, and were confirmed to round-trip through the image's own
parser with every key intact.

Three consequences that shape the pod specs:

* **`/opt/flink/conf` must be writable.** The entrypoint *rewrites* config.yaml in
  place to apply `JOB_MANAGER_RPC_ADDRESS` and `blob.server.port`. A ConfigMap volume
  is read-only, so an init container copies the image's own conf directory into an
  emptyDir and then overlays `config.yaml` from the ConfigMap. Copying the whole
  directory rather than mounting only `config.yaml` keeps the `log4j*`/`logback*` files
  Flink's start scripts expect.
* **`JOB_MANAGER_RPC_ADDRESS=flink-jobmanager` is set on both Deployments.** Otherwise
  the entrypoint sets `jobmanager.rpc.address` to the pod's own hostname, which nothing
  in the cluster can resolve.
* **The Prometheus reporter needs no extra jar.** `flink-metrics-prometheus-1.20.5.jar`
  ships in `/opt/flink/plugins/metrics-prometheus/` and is loaded from there, so
  nothing has to be copied into `lib/` at runtime - which matters, because the root
  filesystem is read-only.

TaskManagers are **3 pods x 1 slot**, not 1 pod x 3 slots. Both fill a parallelism-3
job identically, but only the first can be scaled by Kubernetes: an HPA changes replica
counts and cannot reach into a container to change `taskmanager.numberOfTaskSlots`.
Phase 3 scales this Deployment, so the unit of capacity has to be the pod.

### Replica counts are tied to the partition count

`alerts.generated.v1` has 3 partitions and all alert-service replicas share one
consumer group, so 3 is the maximum useful replica count: a fourth would join the
group, be assigned no partition, and idle. It would add query-API capacity but no
consumption capacity - which is exactly the subtlety phase 3's HPA has to be reasoned
about against, rather than scaling on CPU alone.

### Probe timings

A cold Spring Boot JVM on this workstation answers its first actuator request in
roughly 30-45 seconds. That is the number the startup probes are sized against:

| | `initialDelay + failureThreshold × period` | Budget | Margin over 45s |
|---|---|---|---|
| ingest-service | 10 + 30 × 5 | 160s | ~3.5x |
| alert-service | 10 + 40 × 5 | 210s | ~4.7x |

alert-service gets the larger budget because it does more before it can answer: the
same cold JVM start, then Flyway takes the migration lock and applies
`V1__alerts.sql`. With three replicas starting at once, two of them wait on that lock.

While a startup probe is still failing, the liveness probe is not evaluated at all -
which is the entire reason for having a separate startup probe in front of a JVM.
Liveness on its own with a threshold this generous would leave a genuinely hung
process running for three minutes before restarting it.

On shutdown: a 10s `preStop` sleep, then SIGTERM, then Spring's 20s graceful shutdown,
inside a 45s `terminationGracePeriodSeconds`. The sleep is not superstition - endpoint
removal and container termination race, because every node's kube-proxy has to observe
the EndpointSlice change asynchronously. Sleeping first means the process is still
serving while the last requests are still being routed to it.

### Pod Security: `restricted`, everywhere

The namespace enforces `restricted` and every pod satisfies it: `runAsNonRoot`, a
pinned non-root uid and `fsGroup`, `allowPrivilegeEscalation: false`,
`capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault`. No component needed to
be downgraded to `baseline`.

The uids are the ones each image would have dropped to itself, so the privilege drop
happens before the container starts rather than inside it: Kafka `1000` (`appuser`),
PostgreSQL `70` (`postgres`), Flink `9999` (`flink`), and both Spring services `1001`
(the `stream` account created in the Dockerfile).

`readOnlyRootFilesystem: true` is set on **every** container, including the init
containers. The three upstream images that insist on writing somewhere get an emptyDir
for exactly those paths:

| Image | Writable mounts | Why |
|---|---|---|
| Kafka | `/opt/kafka/config`, `/opt/kafka/logs`, `/tmp` | the entrypoint renders `server.properties` from the `KAFKA_*` environment into the config dir, and `kafka-run-class.sh` creates `$LOG_DIR` at start-up |
| PostgreSQL | `/var/run/postgresql`, `/tmp` | the unix socket and temp files; everything else is inside PGDATA on the PVC |
| Flink | `/opt/flink/conf`, `/opt/flink/log`, `/tmp` | conf is rewritten by the entrypoint; `/tmp` holds spilled network buffers and the blob cache |
| Spring services | `/tmp` | `java.io.tmpdir`, the embedded servlet container's work directory |

Every workload has its own ServiceAccount with `automountServiceAccountToken: false`,
on both the ServiceAccount and the pod spec. Nothing here talks to the API server -
that is only needed by Flink's *native* Kubernetes integration, which this deployment
deliberately does not use.

### Anti-affinity is preferred, spread constraints are `ScheduleAnyway`

Both are soft on purpose. A hard `requiredDuringScheduling` anti-affinity or a
`DoNotSchedule` spread constraint would leave two of three Kafka brokers and two of
three TaskManagers `Pending` forever on a single-node minikube. On a multi-node cluster
the scheduler honours them anyway, which is the only place they matter.

### Secrets

The database credentials live in one `Secret` (`stream-postgres`). PostgreSQL reads it
via `envFrom`; alert-service references the same two keys via `secretKeyRef`. Neither
workload carries the password in its own spec, and no ConfigMap in this tree contains
a credential. `SPRING_DATASOURCE_URL` is in a ConfigMap because it is wiring, not a
secret.

---

## Deliberate scope boundaries

These are choices, not oversights. Each is a project in its own right and none of them
is what the dissertation measures.

1. **Single-replica PostgreSQL.** Replicated PostgreSQL means streaming replication, a
   failover election, and connection routing that survives a promotion. The subject
   here is the streaming pipeline's latency and scaling behaviour, not the alert
   store's availability. Consequences accepted: the alert query API has a single point
   of failure, and a node drain interrupts it. There is deliberately no
   PodDisruptionBudget for it either - over one replica, any budget either permits a
   total outage or blocks every drain.

2. **No Flink JobManager HA.** Real HA needs a ZooKeeper or Kubernetes-based
   high-availability service plus durable, shared HA storage. Without it, a JobManager
   restart restarts the job from scratch rather than recovering from the last
   checkpoint. Half-implementing it - configuring `high-availability` without durable
   storage behind it - would be worse than not doing it, because it would look
   configured. The JobManager Deployment uses `strategy: Recreate` precisely because
   two JobManagers without HA would both try to own the same job.

3. **Checkpoint storage is a filesystem PVC, not object storage.** `ReadWriteMany` on
   minikube's hostpath provisioner works because every pod is on one node. On a
   genuinely multi-node cluster the `staging` overlay needs an RWX-capable CSI driver,
   or - the production answer - `state.checkpoints.dir` and `state.savepoints.dir`
   pointed at `s3://` with `flink-s3-fs-presto` enabled as a plugin.

4. **Credentials are committed in clear.** `base/postgres/secret.yaml` holds
   development credentials so that `kubectl apply -k` is self-contained. Every consumer
   already reads them through a `Secret` reference, so swapping in Sealed Secrets,
   External Secrets or a cloud secret store is a change to that one object.

5. **PLAINTEXT everywhere.** No TLS, no SASL, no NetworkPolicies. All traffic is
   in-namespace and the cluster is a workstation minikube or a private staging cluster.

6. **No Ingress.** NodePorts on `local`, nothing published on `staging`. An Ingress
   controller is a cluster-level dependency that would have to be assumed rather than
   declared here.

7. **No HPA and no Prometheus yet.** Both are phase 3. What is in place for them: every
   workload has resource requests (which an HPA needs to compute utilisation),
   `metrics-server` is enabled by the recommended `minikube start`, all four Services
   carry a `monitoring: stream` label for a `ServiceMonitor` selector, and the Spring
   and Flink pods carry Prometheus scrape annotations.

8. **`kubectl apply -k`, not Argo CD or Flux.** Phase 2's Jenkins pipeline calls
   `kustomize edit set image` against the overlay and then applies it. The `images:`
   block in each overlay's `kustomization.yaml` exists for exactly that.

---

## Validation

Everything here is validated statically. None of it has been applied to a cluster -
that is the operator's call.

```bash
kubectl kustomize deploy/k8s/overlays/local       # renders 31 objects
kubectl kustomize deploy/k8s/overlays/staging     # renders 34 objects
kubectl create --dry-run=client --validate=false -k deploy/k8s/overlays/local
```

`kubectl apply --dry-run=client` cannot be run without a reachable API server: `apply`
fetches each object's current state to compute a three-way merge, and kubectl also
downloads the OpenAPI schema for validation. `kubectl create --dry-run=client` is the
offline equivalent and exercises the same decoding path.

For real schema validation offline, use
[kubeconform](https://github.com/yannh/kubeconform):

```bash
kubectl kustomize deploy/k8s/overlays/local   | kubeconform -strict -summary -kubernetes-version 1.32.0
kubectl kustomize deploy/k8s/overlays/staging | kubeconform -strict -summary -kubernetes-version 1.32.0
```

Both currently report `Valid: 31, Invalid: 0` and `Valid: 34, Invalid: 0`.
