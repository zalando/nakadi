stop-and-remove:
	-docker stop local-kafka && docker rm local-kafka

build-kafka-docker-image: Dockerfile.kafka install-local-kafka.sh start-local-kafka.sh stop-and-remove
	docker build --file Dockerfile.kafka -t local-kafka .

run: build-kafka-docker-image
	docker run -p 9092:9092 -p 2181:2181 --name local-kafka -d local-kafka
