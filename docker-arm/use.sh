set -e
pushd ..
./gradlew assemble
popd
docker-compose down
docker-compose up -d --build
