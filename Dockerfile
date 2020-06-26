FROM registry.opensource.zalan.do/stups/openjdk:1.8.0-191-19

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD app/build/libs/app.jar nakadi.jar
COPY app/api/nakadi-event-bus-api.yaml api/nakadi-event-bus-api.yaml

EXPOSE 8080

ENTRYPOINT exec java -Djava.security.egd=file:/dev/./urandom -Dspring.jdbc.getParameterType.ignore=true -jar nakadi.jar
