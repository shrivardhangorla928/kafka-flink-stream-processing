// Build, test, analyse and deploy the stream processing services to minikube.

pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 45, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '10'))
        disableConcurrentBuilds()
    }

    environment {
        // outside the workspace so cleanWs doesn't wipe the maven cache
        MAVEN_REPO = '/var/jenkins_home/.m2/repository'
        MAVEN_OPTS = '-Xmx1536m'

        SONAR_PROJECT_KEY = 'stream-processing'

        // new tag every build, otherwise k8s sees no change and skips the rollout
        IMAGE_TAG     = "1.0.0-b${BUILD_NUMBER}"
        K8S_NAMESPACE = 'stream'
        K8S_OVERLAY   = 'deploy/k8s/overlays/local'

        MINIKUBE_IP     = '192.168.49.2'
        INGEST_NODEPORT = '30091'
        ALERT_NODEPORT  = '30092'
        FLINK_NODEPORT  = '30081'
    }

    stages {

        stage('Checkout') {
            steps {
                script {
                    def sha = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    def branch = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()
                    def msg = sh(script: 'git log -1 --pretty=%s', returnStdout: true).trim()
                    currentBuild.displayName = "#${BUILD_NUMBER} ${sha}"
                    currentBuild.description = msg
                    echo "branch=${branch} sha=${sha} imageTag=${IMAGE_TAG}"
                }
                sh 'java -version; mvn -v; docker version --format "client={{.Client.Version}} server={{.Server.Version}}"; kubectl version --client=true -o yaml | head -5; kustomize version'
            }
        }

        stage('Build & Test') {
            steps {
                // verify, not test, so the JaCoCo report is generated
                sh "mvn -B -ntp -Dmaven.repo.local=${MAVEN_REPO} clean verify"
            }
            post {
                always {
                    junit allowEmptyResults: false,
                          testResults: '**/target/surefire-reports/TEST-*.xml,**/target/failsafe-reports/TEST-*.xml'
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
                        echo 'SKIP_QUALITY_GATE is set, not enforcing the gate.'
                        return
                    }
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
                // DOCKER_HOST points at minikube, so the kubelet sees these images directly
                sh '''
                    set -eu
                    for pair in ingest:ingest-service alert:alert-service flinkjob:flink-pipeline; do
                        target="${pair%%:*}"
                        name="${pair##*:}"
                        echo "--- stream/$name:$IMAGE_TAG"
                        docker build -f deploy/docker/Dockerfile --target "$target" -t "stream/$name:$IMAGE_TAG" .
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
                    # kubeconfig is mounted read-only, so no use-context here
                    kubectl get ns $K8S_NAMESPACE

                    cd $K8S_OVERLAY
                    kustomize edit set image stream/ingest-service=stream/ingest-service:$IMAGE_TAG
                    kustomize edit set image stream/alert-service=stream/alert-service:$IMAGE_TAG
                    kustomize edit set image stream/flink-pipeline=stream/flink-pipeline:$IMAGE_TAG
                    cd - >/dev/null
                    kubectl kustomize $K8S_OVERLAY | grep -E "^\\s+image:" | sort -u

                    # a finished job can't be re-applied, so remove it first
                    kubectl -n $K8S_NAMESPACE delete job kafka-topic-init --ignore-not-found
                    kubectl apply -k $K8S_OVERLAY
                '''
                sh '''
                    set -eu
                    kubectl -n $K8S_NAMESPACE rollout status statefulset/kafka --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status statefulset/postgres --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/flink-jobmanager --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/flink-taskmanager --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/ingest-service --timeout=300s
                    kubectl -n $K8S_NAMESPACE rollout status deployment/alert-service --timeout=300s
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

                    echo "--- health"
                    curl -fsS "http://$MINIKUBE_IP:$INGEST_NODEPORT/actuator/health" | jq -e '.status == "UP"'
                    curl -fsS "http://$MINIKUBE_IP:$ALERT_NODEPORT/actuator/health" | jq -e '.status == "UP"'

                    echo "--- topics"
                    kubectl -n $K8S_NAMESPACE exec kafka-0 -- \
                      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | sort | tee /tmp/topics.txt
                    for t in telemetry.raw.v1 telemetry.aggregated.v1 alerts.generated.v1 telemetry.dlq.v1; do
                        grep -qx "$t" /tmp/topics.txt || { echo "missing topic: $t"; exit 1; }
                    done

                    echo "--- flink job"
                    curl -fsS "http://$MINIKUBE_IP:$FLINK_NODEPORT/jobs/overview" \
                      | jq -e '(.jobs | map(select(.state == "RUNNING")) | length) > 0'
                '''
                sh '''
                    set -eu

                    pg() {
                        kubectl -n $K8S_NAMESPACE exec postgres-0 -- \
                          psql -U stream -d stream -tAc "$1" 2>/dev/null | tr -d "[:space:]"
                    }

                    station="SMOKE-$BUILD_NUMBER"
                    before=$(pg 'SELECT count(*) FROM station_window_aggregates;')
                    echo "--- aggregates before: ${before:-0}"

                    # 250 mm is above the 24 hour extreme limit, so this should raise an EXTREME alert
                    echo "--- posting a test reading for $station"
                    curl -fsS -X POST "http://$MINIKUBE_IP:$INGEST_NODEPORT/api/v1/telemetry/readings" \
                      -H 'Content-Type: application/json' \
                      -d '{"stationId":"'"$station"'","sensorType":"RAINFALL","value":250.0}'
                    echo

                    deadline=$(( $(date +%s) + 120 ))
                    until curl -fsS "http://$MINIKUBE_IP:$ALERT_NODEPORT/api/v1/alerts?stationId=$station" \
                          | jq -e '(.content | length) > 0 and .content[0].severity == "EXTREME"' >/dev/null; do
                        if [ "$(date +%s)" -ge "$deadline" ]; then
                            echo "FAIL: no EXTREME alert for $station after 120s"
                            exit 1
                        fi
                        sleep 5
                    done
                    echo "PASS: EXTREME alert created for $station"

                    after=$(pg 'SELECT count(*) FROM station_window_aggregates;')
                    if [ "${after:-0}" -le "${before:-0}" ]; then
                        echo "FAIL: aggregates did not grow (${before:-0} -> ${after:-0})"
                        exit 1
                    fi
                    echo "PASS: aggregates grew ${before:-0} -> ${after:-0}"

                    dlq=$(kubectl -n $K8S_NAMESPACE exec kafka-0 -- \
                      /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic telemetry.dlq.v1 2>/dev/null \
                      | awk -F: '{s+=$3} END {print s+0}')
                    echo "telemetry.dlq.v1 depth: $dlq"
                    [ "${dlq:-0}" -eq 0 ] || echo "WARNING: $dlq records were dead-lettered"
                '''
            }
        }
    }

    post {
        always {
            script {
                if (params.DEPLOY) {
                    // keep pod logs and events, they're gone once the pods are replaced
                    sh """
                        mkdir -p diagnostics
                        kubectl -n ${K8S_NAMESPACE} get all,pvc -o wide > diagnostics/resources.txt 2>&1 || true
                        kubectl -n ${K8S_NAMESPACE} describe pods > diagnostics/describe-pods.txt 2>&1 || true
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
            echo "Deployed stream/*:${IMAGE_TAG} to ${K8S_NAMESPACE}."
        }
        failure {
            echo "Build failed. Sonar results: /dashboard?id=${SONAR_PROJECT_KEY}"
        }
    }
}
