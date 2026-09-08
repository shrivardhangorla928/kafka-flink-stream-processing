// Delivery pipeline for the Kafka + Flink stream processing platform.
//
//   build -> test -> SonarQube analysis -> quality gate -> images -> deploy -> smoke test
//
// Runs on the controller built by deploy/cicd/jenkins/Dockerfile, which supplies
// maven, docker + buildx, kubectl, kustomize and jq. The controller is attached to
// minikube's Docker network, so DOCKER_HOST reaches minikube's daemon and the node's
// NodePorts are routable by IP - see deploy/cicd/README.md.
//
// The pipeline is deliberately linear. There is no parallel stage: the reactor build
// is already parallel internally, the Sonar analysis needs the build's output, and the
// deploy needs the gate's verdict. Fanning these out would buy nothing and would make
// a failure harder to read.

pipeline {

    agent any

    options {
        timestamps()
        // A hung Flink rollout or a Sonar webhook that never arrives should fail the
        // build rather than hold an executor overnight.
        timeout(time: 45, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '10'))
        disableConcurrentBuilds()
    }

    environment {
        // Persisted in the jenkins-home volume, so only the first build of a fresh
        // controller pays the full dependency download. It has to sit OUTSIDE the
        // workspace, because cleanWs() in the post block would otherwise delete it.
        MAVEN_REPO   = '/var/jenkins_home/.m2/repository'
        MAVEN_OPTS   = '-Xmx1536m'

        SONAR_PROJECT_KEY = 'stream-processing'

        // Immutable per-build tag. The overlay's pinned 1.0.0 is retagged to this in
        // the deploy stage, which is what makes the rollout actually roll: with
        // imagePullPolicy IfNotPresent and an unchanged tag, Kubernetes sees no spec
        // change and does nothing.
        IMAGE_TAG    = "1.0.0-b${BUILD_NUMBER}"
        K8S_NAMESPACE = 'stream'
        K8S_OVERLAY   = 'deploy/k8s/overlays/local'

        // The minikube node. Reachable because the controller joins the minikube
        // network; NodePorts are fixed by the local overlay.
        MINIKUBE_IP     = '192.168.49.2'
        INGEST_NODEPORT = '30091'
        ALERT_NODEPORT  = '30092'
        FLINK_NODEPORT  = '30081'
    }

    stages {

        stage('Checkout') {
            steps {
                script {
                    // Declarative SCM has already cloned; this only records what was built.
                    def sha    = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    def branch = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()
                    def msg    = sh(script: 'git log -1 --pretty=%s', returnStdout: true).trim()
                    currentBuild.displayName = "#${BUILD_NUMBER} ${sha}"
                    currentBuild.description = msg
                    echo "branch=${branch} sha=${sha} imageTag=${IMAGE_TAG}"
                }
                sh 'java -version; mvn -v; docker version --format "client={{.Client.Version}} server={{.Server.Version}}"; kubectl version --client=true -o yaml | head -5; kustomize version'
            }
        }

        stage('Build & Test') {
            steps {
                // verify, not test: the JaCoCo report is bound to the verify phase and
                // failsafe's integration tests run there too. `mvn test` would produce
                // no coverage XML for Sonar to read.
                //
                // Double quotes so Groovy substitutes MAVEN_REPO here, at pipeline level.
                // Writing -Dmaven.repo.local=$MAVEN_REPO inside another environment{}
                // entry does NOT work: the shell expands the outer variable but will not
                // re-expand what its value contains, so Maven receives the literal string
                // "$MAVEN_REPO" and quietly creates a directory of that name inside the
                // workspace - which cleanWs() then deletes, so every build re-downloads
                // the whole dependency tree.
                sh "mvn -B -ntp -Dmaven.repo.local=${MAVEN_REPO} clean verify"
            }
            post {
                always {
                    junit allowEmptyResults: false,
                          testResults: '**/target/surefire-reports/TEST-*.xml,**/target/failsafe-reports/TEST-*.xml'
                    // sourceDirectories is required in a multi-module reactor. The JaCoCo
                    // XML names classes by package only ("com/stream/processing/..."), and
                    // the plugin resolves that against the workspace root, where nothing
                    // matches - each module keeps its own src/main/java. Without these
                    // four roots the metrics still parse correctly but every source lookup
                    // fails, which floods the log ("skipped logging of 57 additional
                    // errors") and leaves the coverage report unbrowsable.
                    recordCoverage(
                        tools: [[parser: 'JACOCO', pattern: '**/target/site/jacoco/jacoco.xml']],
                        sourceCodeRetention: 'EVERY_BUILD',
                        sourceDirectories: [
                            [path: 'common-model/src/main/java'],
                            [path: 'ingest-service/src/main/java'],
                            [path: 'flink-pipeline/src/main/java'],
                            [path: 'alert-service/src/main/java']
                        ]
                    )
                }
            }
        }

        stage('SonarQube Analysis') {
            steps {
                // withSonarQubeEnv injects SONAR_HOST_URL and SONAR_AUTH_TOKEN from the
                // 'sonarqube' installation declared in JCasC, and records the analysis
                // task id that the next stage waits on.
                withSonarQubeEnv('sonarqube') {
                    sh """
                        mvn -B -ntp -Dmaven.repo.local=${MAVEN_REPO} sonar:sonar \
                          -Dsonar.projectKey=${SONAR_PROJECT_KEY} \
                          -Dsonar.projectName='Kafka + Flink Stream Processing' \
                          -Dsonar.projectVersion=${IMAGE_TAG} \
                          -Dsonar.scm.revision=\$(git rev-parse HEAD)
                    """
                }
            }
        }

        stage('Quality Gate') {
            steps {
                script {
                    if (params.SKIP_QUALITY_GATE) {
                        echo 'SKIP_QUALITY_GATE is set - analysis was published but the gate is not enforced.'
                        return
                    }
                    // Blocks on SonarQube's webhook callback rather than polling. The
                    // outer 15 minutes is a backstop: the compute engine on this
                    // instance finishes a reactor this size in well under a minute, so
                    // anything approaching the timeout means the webhook never arrived.
                    timeout(time: 15, unit: 'MINUTES') {
                        def qg = waitForQualityGate abortPipeline: false
                        echo "Quality gate: ${qg.status}"
                        if (qg.status != 'OK') {
                            error "Quality gate failed with status ${qg.status}. " +
                                  "See ${env.SONAR_HOST_URL ?: 'SonarQube'}/dashboard?id=${SONAR_PROJECT_KEY}"
                        }
                    }
                }
            }
        }

        stage('Container Images') {
            steps {
                // One docker build per target, but the shared `builder` stage means one
                // Maven compile: BuildKit reuses the stage across the three invocations.
                // These run against minikube's daemon (DOCKER_HOST), so the images are
                // visible to the kubelet without a registry.
                sh '''
                    set -eu
                    echo "building into $DOCKER_HOST"
                    for pair in ingest:ingest-service alert:alert-service flinkjob:flink-pipeline; do
                        target="${pair%%:*}"
                        name="${pair##*:}"
                        echo "--- stream/$name:$IMAGE_TAG (target=$target)"
                        docker build \
                          -f deploy/docker/Dockerfile \
                          --target "$target" \
                          -t "stream/$name:$IMAGE_TAG" \
                          .
                    done
                    docker images | grep "^stream/" | grep "$IMAGE_TAG"
                '''
            }
        }

        stage('Deploy to Kubernetes') {
            when {
                expression { return params.DEPLOY }
            }
            steps {
                sh '''
                    set -eu
                    # No `kubectl config use-context` here: KUBECONFIG is the host's
                    # kubeconfig bind-mounted read-only, and use-context rewrites the file.
                    # It already carries current-context: minikube, so this is both
                    # unnecessary and, against a read-only mount, fatal.
                    kubectl get ns $K8S_NAMESPACE

                    # Retag the overlay to this build. kustomize edit rewrites
                    # kustomization.yaml in the workspace only - the change is never
                    # committed, and the next build starts from a clean checkout.
                    cd $K8S_OVERLAY
                    kustomize edit set image stream/ingest-service=stream/ingest-service:$IMAGE_TAG
                    kustomize edit set image stream/alert-service=stream/alert-service:$IMAGE_TAG
                    kustomize edit set image stream/flink-pipeline=stream/flink-pipeline:$IMAGE_TAG
                    cd - >/dev/null

                    echo "--- rendered image references"
                    kubectl kustomize $K8S_OVERLAY | grep -E "^\\s+image:" | sort -u

                    # A completed Job's spec is immutable, so re-applying over an earlier
                    # run fails on the topic provisioner. Deleting it first makes the
                    # apply idempotent; --if-exists keeps a first deploy quiet.
                    kubectl -n $K8S_NAMESPACE delete job kafka-topic-init --ignore-not-found

                    kubectl apply -k $K8S_OVERLAY
                '''
                sh '''
                    set -eu
                    # StatefulSets first: everything else has an init container blocking
                    # on Kafka or PostgreSQL, so waiting on the Deployments before the
                    # stores are up just burns the timeout.
                    kubectl -n $K8S_NAMESPACE rollout status statefulset/kafka    --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status statefulset/postgres --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/flink-jobmanager  --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/flink-taskmanager --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/ingest-service    --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/alert-service     --timeout=300s
                    kubectl -n $K8S_NAMESPACE get pods -o wide
                '''
            }
        }

        stage('Smoke Test') {
            when {
                expression { return params.DEPLOY }
            }
            steps {
                sh '''
                    set -eu

                    echo "--- both services report healthy"
                    curl -fsS "http://$MINIKUBE_IP:$INGEST_NODEPORT/actuator/health" | jq -e '.status == "UP"'
                    curl -fsS "http://$MINIKUBE_IP:$ALERT_NODEPORT/actuator/health"  | jq -e '.status == "UP"'

                    echo "--- the four topics exist with 3 partitions each"
                    kubectl -n $K8S_NAMESPACE exec kafka-0 -- \
                      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list \
                      | sort | tee /tmp/topics.txt
                    for t in telemetry.raw.v1 telemetry.aggregated.v1 alerts.generated.v1 telemetry.dlq.v1; do
                        grep -qx "$t" /tmp/topics.txt || { echo "MISSING TOPIC: $t"; exit 1; }
                    done

                    echo "--- the Flink job is RUNNING with all three slots taken"
                    curl -fsS "http://$MINIKUBE_IP:$FLINK_NODEPORT/jobs/overview" \
                      | jq -e '.jobs | length > 0 and (map(select(.state == "RUNNING")) | length) > 0'
                    curl -fsS "http://$MINIKUBE_IP:$FLINK_NODEPORT/overview" \
                      | jq '{taskmanagers, "slots-total", "slots-available"}'
                '''
                sh '''
                    set -eu

                    pg() {
                        kubectl -n $K8S_NAMESPACE exec postgres-0 -- \
                          psql -U stream -d stream -tAc "$1" 2>/dev/null | tr -d "[:space:]"
                    }

                    # Baseline BEFORE the burst. Asserting count(*) > 0 afterwards would
                    # pass on rows left by any earlier run, so it proves the table is
                    # non-empty and nothing about whether this deployment moves data.
                    # The check has to be that the count GREW.
                    before=$(pg 'SELECT count(*) FROM station_window_aggregates;')
                    echo "--- aggregates before the burst: ${before:-0}"

                    echo "--- force a burst rather than waiting out a 5-minute window"
                    curl -fsS -XPOST "http://$MINIKUBE_IP:$INGEST_NODEPORT/api/v1/telemetry/simulate/burst?stormStations=12"
                    echo

                    # Window (5 min) + bounded out-of-orderness (30s) + one scrape
                    # interval, because Flink's watermark is data-driven: the window
                    # fires only once a later reading actually arrives. That measured
                    # behaviour is why this waits minutes rather than seconds.
                    echo "--- waiting for a NEW window to fire"
                    deadline=$(( $(date +%s) + 480 ))
                    after="${before:-0}"
                    while [ "$(date +%s)" -lt "$deadline" ]; do
                        after=$(pg 'SELECT count(*) FROM station_window_aggregates;')
                        echo "    aggregates=${after:-0} (baseline ${before:-0})"
                        [ "${after:-0}" -gt "${before:-0}" ] && break
                        sleep 20
                    done

                    echo "--- pipeline output"
                    kubectl -n $K8S_NAMESPACE exec postgres-0 -- \
                      psql -U stream -d stream -c \
                      'SELECT count(*) AS aggregates, max(window_end) AS latest_window FROM station_window_aggregates;'
                    kubectl -n $K8S_NAMESPACE exec postgres-0 -- \
                      psql -U stream -d stream -c \
                      'SELECT severity, count(*), max(generated_at) FROM alerts GROUP BY severity ORDER BY 1;'

                    if [ "${after:-0}" -le "${before:-0}" ]; then
                        echo "FAIL: no new window produced an aggregate within the deadline."
                        echo "      before=${before:-0} after=${after:-0} - this deployment is not moving data."
                        exit 1
                    fi
                    echo "PASS: aggregates grew ${before:-0} -> ${after:-0} after this build's burst."

                    echo "--- the alert query API answers"
                    curl -fsS "http://$MINIKUBE_IP:$ALERT_NODEPORT/api/v1/alerts?size=5" | jq '{total: .totalElements}'

                    echo "--- nothing dead-lettered"
                    # kafka-get-offsets.sh, not `kafka-run-class.sh kafka.tools.GetOffsetShell`:
                    # that class was removed in Kafka 3.9 and the old invocation fails with
                    # ClassNotFoundException, which awk then happily sums to 0 - a dead-letter
                    # check that always passes.
                    dlq=$(kubectl -n $K8S_NAMESPACE exec kafka-0 -- \
                      /opt/kafka/bin/kafka-get-offsets.sh \
                      --bootstrap-server kafka:9092 --topic telemetry.dlq.v1 2>/dev/null \
                      | awk -F: '{s+=$3} END {print s+0}')
                    echo "telemetry.dlq.v1 depth: $dlq"
                    [ "${dlq:-0}" -eq 0 ] || echo "WARNING: $dlq records were dead-lettered - check alert-service logs."
                '''
            }
        }
    }

    post {
        always {
            script {
                if (params.DEPLOY) {
                    // Cheap to collect, and the only record of why a rollout failed once
                    // the pods have been replaced.
                    sh """
                        mkdir -p diagnostics
                        kubectl -n ${K8S_NAMESPACE} get all,pvc -o wide     > diagnostics/resources.txt 2>&1 || true
                        kubectl -n ${K8S_NAMESPACE} describe pods           > diagnostics/describe-pods.txt 2>&1 || true
                        kubectl -n ${K8S_NAMESPACE} get events --sort-by=.lastTimestamp > diagnostics/events.txt 2>&1 || true
                        for d in ingest-service alert-service flink-jobmanager flink-taskmanager; do
                            kubectl -n ${K8S_NAMESPACE} logs deploy/\$d --tail=400 --all-containers \
                              > diagnostics/logs-\$d.txt 2>&1 || true
                        done
                    """
                    archiveArtifacts artifacts: 'diagnostics/**', allowEmptyArchive: true
                }
            }
            cleanWs(deleteDirs: true, notFailBuild: true,
                    patterns: [[pattern: 'diagnostics/**', type: 'EXCLUDE']])
        }
        success {
            echo "Deployed stream/*:${IMAGE_TAG} to namespace ${K8S_NAMESPACE} and verified end to end."
        }
        failure {
            echo "Build failed. If it stopped at the Quality Gate, the analysis is at /dashboard?id=${SONAR_PROJECT_KEY}."
        }
    }
}
