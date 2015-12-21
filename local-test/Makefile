
ifeq ($(shell uname), Darwin)
	# we should pass the docker-machine IP address down to the Docker container
	# for Kafka to be able to advertize the IP address it is accessible from
	BUILD_ARG=--build-arg KAFKA_ADVERTISED_HOST_NAME=$(shell docker-machine ip default)
endif


build-kafka-docker-image: Dockerfile.kafka install-local-kafka.sh start-local-kafka.sh
	docker build $(BUILD_ARG) --file Dockerfile.kafka -t local-kafka .

run: build-kafka-docker-image
	docker run -p 9092:9092 -p 2181:2181 -d local-kafka

interactive: build-kafka-docker-image
	docker run -p 9092:9092 -p 2181:2181 -it local-kafka

DOCKER_ID=$(shell docker ps -qf "ancestor=local-kafka")

kill:
ifneq ($(DOCKER_ID),)
	@echo "Killing container $(shell docker kill $(DOCKER_ID))"
endif

