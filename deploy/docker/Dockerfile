# Multi-stage, multi-target build for every deployable in the platform.
#
# One `builder` stage compiles the whole reactor once; each runtime target then copies
# only the artefact it needs. Building the three images therefore costs one Maven run
# rather than three, and the dependency layer is shared and cached across all of them.
#
# Build context is the project root, e.g.:
#   docker build -f deploy/docker/Dockerfile --target ingest -t stream/ingest-service:1.0.0 .

# ---------------------------------------------------------------------------
# Stage 1 - dependency cache
# ---------------------------------------------------------------------------
FROM maven:3.9.9-eclipse-temurin-21 AS deps

WORKDIR /build

# Copy only the POMs first so that a source-only change does not invalidate the
# dependency download layer - this is what keeps the Jenkins build fast later.
COPY pom.xml ./
COPY common-model/pom.xml   common-model/pom.xml
COPY ingest-service/pom.xml ingest-service/pom.xml
COPY flink-pipeline/pom.xml flink-pipeline/pom.xml
COPY alert-service/pom.xml  alert-service/pom.xml

RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -ntp dependency:go-offline -DskipTests

# ---------------------------------------------------------------------------
# Stage 2 - compile and package
# ---------------------------------------------------------------------------
FROM deps AS builder

COPY common-model   common-model
COPY ingest-service ingest-service
COPY flink-pipeline flink-pipeline
COPY alert-service  alert-service

ARG SKIP_TESTS=true
RUN --mount=type=cache,target=/root/.m2/repository \
    mvn -B -ntp package -DskipTests=${SKIP_TESTS}

# ---------------------------------------------------------------------------
# Stage 3a - ingest service
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS ingest

# curl is what both the Compose healthcheck and the Kubernetes probes shell out to.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 1001 stream \
 && useradd --system --uid 1001 --gid stream --home-dir /app --no-create-home stream \
 && mkdir -p /app && chown stream:stream /app

WORKDIR /app
COPY --from=builder --chown=stream:stream /build/ingest-service/target/ingest-service.jar app.jar

USER stream
EXPOSE 8080

# MaxRAMPercentage rather than a fixed -Xmx: the JVM then honours whatever limit the
# container runtime or the Kubernetes resource limit imposes, without rebuilding the image.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]

# ---------------------------------------------------------------------------
# Stage 3b - alert service
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS alert

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --system --gid 1001 stream \
 && useradd --system --uid 1001 --gid stream --home-dir /app --no-create-home stream \
 && mkdir -p /app && chown stream:stream /app

WORKDIR /app
COPY --from=builder --chown=stream:stream /build/alert-service/target/alert-service.jar app.jar

USER stream
EXPOSE 8080

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]

# ---------------------------------------------------------------------------
# Stage 3c - Flink job artefact
#
# The job jar is baked into a Flink image rather than submitted from outside, so the
# same immutable image can run as a session-cluster submission here and as an
# application-mode deployment on Kubernetes.
# ---------------------------------------------------------------------------
FROM flink:1.20.5-scala_2.12-java17 AS flinkjob

COPY --from=builder /build/flink-pipeline/target/flink-pipeline.jar /opt/flink/usrlib/flink-pipeline.jar

# Create the checkpoint/savepoint root inside the image, owned by the user the entrypoint drops
# to. Docker seeds a new named volume from the image's directory including its ownership, so
# without this the volume is created root-owned and the JobManager - which runs as `flink` - fails
# at startup with "Failed to create directory for shared state".
RUN mkdir -p /flink-state/checkpoints /flink-state/savepoints \
 && chown -R flink:flink /flink-state
