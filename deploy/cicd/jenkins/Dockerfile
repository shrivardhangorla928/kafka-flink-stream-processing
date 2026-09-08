# Jenkins controller for this project's pipeline.
#
# The stock jenkins/jenkins image carries a JDK and git and nothing else. This build
# adds the three tools the Jenkinsfile actually shells out to, pinned rather than
# installed by a Jenkins "tool installer", so a build is reproducible from the image
# tag alone and does not depend on Jenkins' global tool configuration surviving a
# restart:
#
#   maven    - the reactor build, the JaCoCo report and the Sonar scanner run
#   docker   - the CLI only. There is no daemon in here; DOCKER_HOST points at
#              minikube's daemon so images land where the kubelet can see them.
#   kubectl  - the deploy stage. Kustomize v5 is built in (kubectl kustomize).
#
# Pinned to the same JDK the services target (21) so `mvn verify` inside Jenkins
# compiles against the same release the Dockerfile's builder stage does.
FROM jenkins/jenkins:2.568.3-lts-jdk21

USER root

ARG MAVEN_VERSION=3.9.9
ARG KUBECTL_VERSION=v1.32.3
ARG DOCKER_CLI_VERSION=27.3.1
ARG BUILDX_VERSION=v0.19.3
ARG KUSTOMIZE_VERSION=v5.5.0

# ---------------------------------------------------------------------------
# Maven
# ---------------------------------------------------------------------------
RUN set -eux; \
    curl -fsSL "https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz" \
      -o /tmp/maven.tar.gz; \
    mkdir -p /opt/maven; \
    tar -xzf /tmp/maven.tar.gz -C /opt/maven --strip-components=1; \
    rm /tmp/maven.tar.gz; \
    ln -s /opt/maven/bin/mvn /usr/local/bin/mvn; \
    mvn -v

# ---------------------------------------------------------------------------
# Docker CLI (client only - no daemon, no dind)
# ---------------------------------------------------------------------------
RUN set -eux; \
    curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
      -o /tmp/docker.tgz; \
    tar -xzf /tmp/docker.tgz -C /tmp; \
    mv /tmp/docker/docker /usr/local/bin/docker; \
    rm -rf /tmp/docker /tmp/docker.tgz; \
    docker --version

# ---------------------------------------------------------------------------
# docker buildx
#
# Not optional. deploy/docker/Dockerfile uses `RUN --mount=type=cache` for the
# Maven repository, which is a BuildKit feature; the legacy builder rejects the
# syntax outright ("the --mount option requires BuildKit"). The static CLI tarball
# above ships no plugins, so buildx is installed separately and `docker build`
# then delegates to it. With DOCKER_HOST pointing at a remote daemon and the
# default `docker` driver, the built image is loaded into that daemon - which is
# exactly where the kubelet needs it.
# ---------------------------------------------------------------------------
RUN set -eux; \
    mkdir -p /usr/local/lib/docker/cli-plugins; \
    curl -fsSL "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-amd64" \
      -o /usr/local/lib/docker/cli-plugins/docker-buildx; \
    chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx; \
    docker buildx version

# ---------------------------------------------------------------------------
# kubectl
# ---------------------------------------------------------------------------
RUN set -eux; \
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
      -o /usr/local/bin/kubectl; \
    chmod +x /usr/local/bin/kubectl; \
    kubectl version --client

# ---------------------------------------------------------------------------
# kustomize (standalone)
#
# `kubectl kustomize` can render an overlay but has no `kustomize edit`. The deploy
# stage retags images with `kustomize edit set image`, which the local overlay's own
# kustomization.yaml is written to expect, so the standalone binary is required.
# ---------------------------------------------------------------------------
RUN set -eux; \
    curl -fsSL "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2F${KUSTOMIZE_VERSION}/kustomize_${KUSTOMIZE_VERSION}_linux_amd64.tar.gz" \
      -o /tmp/kustomize.tgz; \
    tar -xzf /tmp/kustomize.tgz -C /usr/local/bin kustomize; \
    rm /tmp/kustomize.tgz; \
    kustomize version

# jq is used by the smoke-test stage to read the Flink REST API
RUN apt-get update \
 && apt-get install -y --no-install-recommends jq \
 && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Plugins
#
# Baked in at build time rather than installed on first boot, so a fresh container
# comes up with a working pipeline and no update-centre round trip.
# ---------------------------------------------------------------------------
COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

# ---------------------------------------------------------------------------
# Configuration as Code
#
# JENKINS_HOME is a volume, so anything configured through the UI is one
# `docker volume rm` away from gone. Everything this project needs - the Sonar
# server, the pipeline job, the security realm - is declared in jenkins.yaml and
# reapplied on every boot.
#
# The file lives OUTSIDE /var/jenkins_home deliberately. Docker seeds a named
# volume from the image only while that volume is empty, so a jenkins.yaml copied
# into JENKINS_HOME is captured by the volume on first boot and every later image
# rebuild is then silently ignored - you edit the config, rebuild, restart, and
# Jenkins keeps applying the old one. Keeping it on the image filesystem proper
# means the running container always has the version that was built.
# ---------------------------------------------------------------------------
COPY jenkins.yaml /usr/local/casc/jenkins.yaml
ENV CASC_JENKINS_CONFIG=/usr/local/casc/jenkins.yaml

# Skip the setup wizard: the admin user and the security realm come from JCasC.
ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=false -Dhudson.model.DownloadService.noSignatureCheck=true"

USER jenkins
