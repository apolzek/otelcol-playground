#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Build the OTLP counter JAR inside a maven container so the host
# does not need a local Java/Maven toolchain. The fat JAR is copied
# into ./flink-jobs/, which the flink-job-submitter scans on startup.

docker run --rm \
    -v "$(pwd)/otlp-counter:/build" \
    -v "${HOME}/.m2:/root/.m2" \
    -w /build \
    maven:3.9-eclipse-temurin-11 \
    mvn -B clean package -DskipTests

cp otlp-counter/target/otlp-counter.jar ./otlp-counter.jar
echo
echo "Built: $(pwd)/otlp-counter.jar"
ls -la otlp-counter.jar
