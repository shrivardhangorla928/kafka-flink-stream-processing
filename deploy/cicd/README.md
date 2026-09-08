# CI/CD: Jenkins + SonarQube

The delivery half of the platform. Jenkins builds the reactor, SonarQube gates it on
code quality, and the same pipeline deploys the result to the Kubernetes cluster set
up in [`../k8s/README.md`](../k8s/README.md) and verifies it end to end.

```
deploy/cicd/
  docker-compose.yml          Jenkins + SonarQube + SonarQube's PostgreSQL
  .env.example                copy to .env; holds the admin passwords and the Sonar token
  jenkins/
    Dockerfile                jenkins:lts-jdk21 + maven, docker CLI, buildx, kubectl, kustomize, jq
    plugins.txt               plugins baked into the image
    jenkins.yaml              Configuration as Code: security, Sonar server, the pipeline job
  sonarqube/
    bootstrap.sh              waits for UP, rotates admin, creates project + token + webhook
    quality-gate.sh           creates the gate and binds it (run after the first analysis)
```

The pipeline itself is [`Jenkinsfile`](../../Jenkinsfile) at the repository root.

---

## Prerequisites

| | |
|---|---|
| Docker | the same daemon that runs minikube |
| A running cluster | `minikube start …` per [`../k8s/README.md`](../k8s/README.md). The Jenkins container attaches to minikube's Docker network, so the cluster must exist before the stack starts. |
| No host tuning | `vm.max_map_count` is deliberately not required — see below. |
| Free host ports | 8100, 9000, 50002 |

### `vm.max_map_count`, and why this stack does not need it

SonarQube embeds Elasticsearch, which by default stores its index with `mmapfs` and
therefore demands `vm.max_map_count >= 262144`. Ubuntu ships 65530. That is a kernel
parameter, it is **not** namespaced, and no container option substitutes for it — the
value the container sees is the host's. The usual symptom is a `sonarqube` container
that starts, answers on 9000 for a few seconds, then exits with
`max virtual memory areas vm.max_map_count [65530] is too low`.

This compose file sidesteps it by setting

```yaml
SONAR_SEARCH_JAVAADDITIONALOPTS: "-Dnode.store.allow_mmap=false"
```

which switches the store to `niofs`. Index reads then go through the filesystem cache
instead of mapped memory. That is a measurable difference on an instance holding
millions of issues and an irrelevant one for a single project's history, so the stack
comes up on a stock box with no root access and no host modification.

If you would rather run the tuned default, raise the sysctl and delete that line:

```bash
sudo sysctl -w vm.max_map_count=262144
echo 'vm.max_map_count=262144' | sudo tee /etc/sysctl.d/99-sonarqube.conf
```

---

## Bring-up

The order matters, because Jenkins reads the SonarQube token at boot and the token
cannot exist until SonarQube is running.

```bash
cd deploy/cicd
cp .env.example .env
# edit .env: set JENKINS_ADMIN_PASSWORD, SONAR_DB_PASSWORD, SONAR_ADMIN_PASSWORD
```

### 1. SonarQube first

```bash
docker compose up -d sonar-db sonarqube
./sonarqube/bootstrap.sh
```

`bootstrap.sh` waits for `/api/system/status` to report `UP` — not just for the port
to open, which happens well before the compute engine can accept a report — then
rotates the shipped `admin/admin` password, creates the project, issues a
`GLOBAL_ANALYSIS_TOKEN`, writes it into `.env`, and registers the Jenkins webhook.

It is idempotent. Re-running it revokes and reissues the token (SonarQube cannot
read a token back after creation) and leaves everything else alone.

### 2. Then Jenkins

```bash
docker compose up -d --build jenkins
```

First build of the image takes a few minutes: it downloads Maven, the Docker CLI,
buildx, kubectl and kustomize, then resolves the plugin closure from `plugins.txt`.

Jenkins comes up at **http://localhost:8100** with the pipeline job
`stream-processing` already created — the job, the Sonar server connection and the
admin user all come from `jenkins/jenkins.yaml`, so nothing is clicked through a
setup wizard.

### 3. Run the pipeline, then set the gate

```
open http://localhost:8100/job/stream-processing/  ->  Build with Parameters
```

Run it once to publish an analysis, then:

```bash
./sonarqube/quality-gate.sh
```

The gate is created *after* the first analysis on purpose — see
[Why the gate is a ratchet](#why-the-gate-is-a-ratchet).

---

## The pipeline

| Stage | What it does | Fails the build when |
|---|---|---|
| Checkout | Records the commit; names the build `#N <sha>` | — |
| Build & Test | `mvn clean verify` — compiles, runs surefire + failsafe, writes the JaCoCo XML. Publishes both to Jenkins. | any test fails |
| SonarQube Analysis | `mvn sonar:sonar` inside `withSonarQubeEnv`, tagged with the build's image tag and git sha | the scanner cannot reach SonarQube |
| Quality Gate | `waitForQualityGate()` — parks the build until SonarQube's webhook delivers the verdict | the gate is not `OK` (unless `SKIP_QUALITY_GATE`) |
| Container Images | Three `docker build` targets against **minikube's** daemon, tagged `1.0.0-b<N>` | any image fails to build |
| Deploy to Kubernetes | `kustomize edit set image` to this build's tag, then `kubectl apply -k` and `rollout status` on all six workloads | any rollout does not complete in 300s |
| Smoke Test | Health of both services, four topics present, Flink job `RUNNING`, then a forced burst and a wait for real rows in PostgreSQL | no window ever produces an aggregate |

`post { always }` collects pod descriptions, events and 400 lines of logs from each
workload into an archived `diagnostics/` artefact — the only record of a failed
rollout once the pods have been replaced.

### Why `verify` and not `test`

JaCoCo's report execution is bound to the `verify` phase and the Testcontainers-based
integration tests run under failsafe, also at `verify`. `mvn test` would pass and
produce no coverage XML at all, so Sonar would report 0% and the gate's coverage
condition would fail for a reason that has nothing to do with the code.

### Why the image tag changes every build

The `local` overlay pins `1.0.0` and sets `imagePullPolicy: IfNotPresent`. Rebuilding
`1.0.0` in place changes the image the tag points at but leaves the Deployment's spec
byte-identical, so Kubernetes sees nothing to do and the old pods keep running with
the old code — a deploy that reports success and changes nothing. Tagging
`1.0.0-b<N>` and retagging the overlay makes the spec change, which is what triggers
the rollout.

---

## How the container reaches the cluster

Two arrangements in `docker-compose.yml` do this, and both are load-bearing.

**1. Jenkins joins the `minikube` network.** Docker isolates bridge networks from
each other by default, so a container on the CI stack's own network cannot route to
`192.168.49.2`. The compose file declares `minikube` as an external network and
attaches Jenkins to both, which makes the node's API server (`:8443`), its Docker
daemon (`:2376`) and the overlay's NodePorts (`30081`, `30091`, `30092`) all
reachable by IP.

**2. The host's `~/.minikube` is bind-mounted at the identical absolute path.** The
kubeconfig minikube writes refers to
`/home/shrivardhan/.minikube/profiles/minikube/client.crt` and friends by absolute
path, and `DOCKER_CERT_PATH` points into the same tree. Mounting it read-only at the
same path inside the container makes every one of those references resolve without
rewriting or copying a single certificate.

There is deliberately **no** `/var/run/docker.sock` mount and no dind sidecar.
Images have to exist in minikube's daemon for the kubelet to find them under
`IfNotPresent`, so `DOCKER_HOST` points there and the host daemon is never involved.

### Why buildx is in the image

`deploy/docker/Dockerfile` uses `RUN --mount=type=cache` to persist the Maven
repository between builds. That is BuildKit syntax; the legacy builder rejects it
with `the --mount option requires BuildKit`. The static Docker CLI tarball ships no
plugins, so buildx is installed explicitly and `docker build` delegates to it.

---

## Why the gate is a ratchet

`quality-gate.sh` sets two groups of conditions.

**On new code** — the standard Sonar way shape: A ratings for reliability, security
and maintainability, 80% coverage, ≤3% duplication. Strict, because holding a small
diff to a high standard is cheap.

**On overall code** — a coverage floor pinned to *the value the project actually
measures*, floored to the whole percent below. Not a round aspiration.

The reason is that a gate configured with an aspirational number fails on the day
it is created, and a pipeline whose gate always fails gets `SKIP_QUALITY_GATE`
ticked permanently and stops meaning anything. Pinned at the measured value, the
gate passes today and fails the first time coverage regresses — which is the only
behaviour that actually protects anything. Raise `COVERAGE_FLOOR` deliberately as
coverage improves; the script takes it as an override.

One caveat the script prints and worth repeating: with no New Code baseline
configured, "new code" is empty, so those five conditions pass trivially. They begin
to bite once a baseline exists to diff against.

---

## Troubleshooting

**SonarQube exits shortly after starting.** `vm.max_map_count`. See above.

**`waitForQualityGate()` hangs until the 15-minute timeout.** The webhook did not
arrive. Check it exists and points at `http://jenkins:8080/sonarqube-webhook/`
(container DNS name, not `localhost`):

```bash
# curl, not wget - the SonarQube image ships no wget
docker compose exec sonarqube \
  curl -fsS -u "admin:$SONAR_ADMIN_PASSWORD" http://localhost:9000/api/webhooks/list
```

Re-run `./sonarqube/bootstrap.sh` to recreate it.

**Jenkins cannot reach the cluster.** Confirm it is on both networks and that the
node answers:

```bash
docker inspect stream-jenkins --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}'
docker compose exec jenkins kubectl --context minikube get nodes
docker compose exec jenkins docker version --format '{{.Server.Version}}'
```

A `minikube stop`/`start` recreates the node's container and can change its IP.
`docker network inspect minikube` shows the current one; if it is not
`192.168.49.2`, update `MINIKUBE_IP` and `DOCKER_HOST`.

**The pipeline job is missing after a restart.** It is declared in `jenkins.yaml`
and recreated on every boot, so a missing job means JCasC did not apply. Check:

```bash
docker compose logs jenkins | grep -i -A5 'configuration-as-code'
```

**Sonar analysis reports 0% coverage.** The build ran `test` rather than `verify`,
or the JaCoCo XML is somewhere other than the paths in the root pom's
`sonar.coverage.jacoco.xmlReportPaths`.

---

## Teardown

```bash
docker compose down                 # keeps build history and analysis
docker compose down -v              # discards both volumes and all history
```

`down` never touches the `minikube` network or the cluster — it is declared external
precisely so that tearing down CI cannot tear down the deployment target.
