#!/bin/bash

CI_REGISTRY=artifactory.aws.wiley.com/docker
CI_REGISTRY_USER=tc-gitlab
CI_REGISTRY_PASSWORD=AKCp8jR6yGwRgeLw4u4sFSUnozrEuCGNFom4Q7TknbVHedJ4G7kkYqzapfZy4XZ59utdArSn8

filter_docker_warning() {
  grep -E -v "^WARNING! Your password will be stored unencrypted in |^Configure a credential helper to remove this warning. See|^https://docs.docker.com/engine/reference/commandline/login/#credentials-store" || true
}

docker_login_filtered() {
  # $1 - username, $2 - password, $3 - registry
  # this filters the stderr of the `docker login`, without merging stdout and stderr together
  { echo "$2" | docker login -u "$1" --password-stdin "$3" 2>&1 1>&3 | filter_docker_warning 1>&2; } 3>&1
}

image_latest=$CI_REGISTRY/tc/api-perftest-1.1.3-all:latest
docker build -t $image_latest .

docker_login_filtered "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" "$CI_REGISTRY"
docker push "$image_latest"
