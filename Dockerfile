FROM registry.opensource.zalan.do/stups/openjdk:8-54

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD build/libs/nakadi.jar nakadi.jar
ADD api/nakadi-event-bus-api.yaml nakadi-event-bus-api.yaml

EXPOSE 8080

ENTRYPOINT exec java \
-Djava.security.egd=file:/dev/./urandom \
-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector \
-jar nakadi.jar
