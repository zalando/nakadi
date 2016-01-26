stop-and-remove:
	-docker stop local-storages && docker rm local-storages

build-storages-docker-image: Dockerfile.storages install-local-storages.sh start-local-storages.sh stop-and-remove
	docker build --file Dockerfile.storages -t local-storages .

run: build-storages-docker-image
	docker run -p 9092:9092 -p 5432:5432 -p 2181:2181 ${PUBLISH_NAKADI_PORT} --name local-storages -d local-storages
