build-kafka-docker-image: Dockerfile.kafka install-local-kafka.sh start-local-kafka.sh
	docker build --file Dockerfile.kafka -t local-kafka .

run: build-kafka-docker-image
	docker run -p 9092:9092 -P --name local-kafka -d local-kafka
