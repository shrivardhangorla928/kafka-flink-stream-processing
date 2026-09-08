# Kafka + Flink Stream Processing

Event-driven **scrape → aggregate → alert** pipeline built on Apache Kafka and Apache Flink,
containerised and deployed on Kubernetes, with a Jenkins/SonarQube delivery pipeline and
Prometheus/Grafana observability.

Dissertation project for BITS Pilani WILP M.Tech (Cloud Computing) —
*DevOps Automation with Scalable Services, Real-Time Monitoring, and Stream Processing*
(Course CCZG628T, BITS ID 2024MT03552), carried out at VassarLabs, Hyderabad.

---

## Why this shape

The organisation's existing hydro-meteorological workflow runs as three coupled batch steps:
a scraper collects station readings, a job aggregates them, and a third step decides which
alerts to raise. Latency is bounded by how often the batch runs, a failure anywhere restarts
the whole chain, and there is no way to scale one stage independently of the others.

This project re-expresses the same workflow as three **independently deployable, independently
scalable stages joined by Kafka topics**, with the aggregation and threshold logic running as
an Apache Flink event-time streaming job:

```
                 ┌──────────────────┐
   stations ────►│  ingest-service  │  Spring Boot, scrapes/receives readings
                 └────────┬─────────┘
                          │  telemetry.raw.v1        (3 partitions, key = stationId)
                          ▼
                 ┌──────────────────────────────────────────────┐
                 │              flink-pipeline                  │
                 │  event-time watermarks (30 s out-of-order)   │
                 │  keyBy(stationId)                            │
                 │  5-minute tumbling window → aggregate        │
                 │  threshold evaluation → alert                │
                 └───┬──────────────────────┬───────────────┬───┘
   telemetry.aggregated.v1        alerts.generated.v1    telemetry.dlq.v1
                     │                      │
                     └──────────┬───────────┘
                                ▼
                       ┌──────────────────┐
                       │  alert-service   │  Spring Boot + PostgreSQL, query API
                       └──────────────────┘
```

Each stage exposes Prometheus metrics, so the pipeline's end-to-end latency
(`windowEnd → alert persisted`) is measurable rather than asserted — that number is the
baseline the evaluation chapter compares against the current batch workflow.

---

## Modules

| Module | What it does | Tests |
|---|---|---|
| `common-model` | The event contract: `SensorReading`, `StationWindowAggregate`, `Alert`, topic names, shared JSON codec. Framework-free, depended on by all three services. | 31 |
| `ingest-service` | Spring Boot. Simulates/receives station telemetry and publishes `SensorReading` to `telemetry.raw.v1`. REST API for pushing readings and triggering bursts. | 83 |
| `flink-pipeline` | Apache Flink job. Event-time windowed aggregation per station, then threshold evaluation, emitting aggregates and alerts. | 100 |
| `alert-service` | Spring Boot + PostgreSQL. Consumes alerts idempotently, persists them, serves the filterable query API. | 109 |

### Java version split

`flink-pipeline` and `common-model` compile to **Java 17** bytecode; `ingest-service` and
`alert-service` compile to **Java 21**. This is not an oversight: Apache Flink publishes no
`java21` container image for the 1.20 LTS line, so anything loaded inside a TaskManager has to be
Java 17 or it fails with `UnsupportedClassVersionError`. A Java 21 module consumes a 17-targeted
jar without issue, so the constraint costs nothing.

### The event contract

| Topic | Payload | Key | Partitions |
|---|---|---|---|
| `telemetry.raw.v1` | `SensorReading` | `stationId` | 3 |
| `telemetry.aggregated.v1` | `StationWindowAggregate` | `stationId` | 3 |
| `alerts.generated.v1` | `Alert` | `stationId` | 3 |
| `telemetry.dlq.v1` | rejected payloads | `stationId` | 3 |

Alert ids are **deterministic** — a UUIDv3 over (station, sensor type, window bounds, severity).
The Flink sink is at-least-once, so a job restored from a checkpoint re-emits windows it had
already emitted; because the id is derived rather than random, the alert store's upsert collapses
the duplicate instead of raising the same flood warning twice.

### Why these numbers

| Setting | Value | Reasoning |
|---|---|---|
| Scrape interval | 60 s | 50 stations × 1/min ≈ 0.83 msg/s at rest; the load-test profile scales this up. |
| Window | 5 min tumbling, event time | 5 readings per station per window at the default scrape interval — enough to make a sum meaningful, short enough to observe alerts during a demo. |
| Watermark lag | 30 s bounded out-of-orderness | A window therefore fires at `windowEnd + 30 s`; that 30 s is the floor on alerting latency. |
| Kafka partitions | 3 | Sets the ceiling on consumer parallelism; matched by the Flink job's parallelism (3), the TaskManager's slot count (3) and the alert service's listener concurrency (3). |
| Checkpoint interval | 30 s, exactly-once | State is small (a few aggregates per station), so checkpoints are cheap relative to the 5-minute window. |

Alerting thresholds (highest breached band wins):

| Sensor | Aggregate | WARNING | SEVERE | EXTREME |
|---|---|---|---|---|
| `RAINFALL` | window sum, mm | 15 | 30 | 50 |
| `RESERVOIR_LEVEL` | window max, % of FRL | 85 | 95 | 100 |
| `RIVER_LEVEL` | window max, m | 8 | 10 | 12 |

---

## Running it locally

Prerequisites: Docker with Compose, JDK 21, Maven 3.9+.

```bash
# build everything and run the full stack
docker compose -f deploy/docker/docker-compose.yml up -d --build

# watch the Flink job come up
open http://localhost:8081
```

| Endpoint | URL |
|---|---|
| Flink web UI | http://localhost:8081 |
| Ingest service | http://localhost:8091 |
| Alert service | http://localhost:8092 |
| Kafka (from the host) | `localhost:29092` |
| PostgreSQL | `localhost:5433`, db/user/password `aware` |

Host ports are shifted off the defaults because this workstation already runs a PostgreSQL on
5432 and a Jenkins container on 8099.

```bash
# force a burst of storm readings so alerts appear without waiting for a window
curl -XPOST 'http://localhost:8091/api/v1/telemetry/simulate/burst?stormStations=10'

# ...then, one window plus the watermark lag later
curl 'http://localhost:8092/api/v1/alerts?severity=WARNING&size=20'
```

### Building without Docker

```bash
mvn -B clean verify          # compiles, tests, and writes JaCoCo coverage reports
```

---

## Project phases

| Phase | Status |
|---|---|
| 1. Streaming application: Kafka + Flink, containerised, on Kubernetes | in progress |
| 2. CI/CD: Jenkins pipeline-as-code with SonarQube quality gates | not started |
| 3. Observability: Prometheus + Grafana, HPA against custom metrics | not started |
| 4. Evaluation: load testing, autoscaling behaviour, latency vs. the batch baseline | not started |
