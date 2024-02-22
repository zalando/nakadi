#!/bin/bash -e

if [ $# != 2 ]; then
    echo "Usage: emergency-build.sh VERSION_TAG INCIDENT_URL"
    exit 1
fi

VERSION_TAG="$1"
INCIDENT_URL="$2"

IMAGE="container-registry-test.zalando.net/aruha/nakadi-base:master-local-${VERSION_TAG}"

./nakadi.sh clean-build

pierone login --url container-registry.zalando.net

rm -f /tmp/cdp-proxy.crt

# Copy cdp-proxy.crt
jdk_container_id=$(docker create container-registry.zalando.net/cdp-runtime/jdk11)
docker cp "$jdk_container_id":/usr/local/share/ca-certificates/cdp/cdp-proxy.crt /tmp
docker rm "$jdk_container_id"

./nakadi.sh create-buildx cdp-buildkitd.toml
./nakadi.sh build-buildx ${IMAGE}

pierone emergency-promotion "$INCIDENT_URL" "$IMAGE"

echo "Nakadi image is built successfully: $IMAGE"
