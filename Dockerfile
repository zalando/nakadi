FROM container-registry.zalando.net/library/eclipse-temurin-11-jdk:latest

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD app/build/libs/app.jar nakadi.jar
COPY app/api/nakadi-event-bus-api.yaml api/nakadi-event-bus-api.yaml
COPY app/api/nakadi-event-bus-api.yaml /zalando-apis/nakadi-event-bus-api.yaml


EXPOSE 8080

ENTRYPOINT exec java -Djava.security.egd=file:/dev/./urandom -Dspring.jdbc.getParameterType.ignore=true -jar nakadi.jar
