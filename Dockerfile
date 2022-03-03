FROM registry.opensource.zalan.do/library/openjdk-11-jre-slim:latest

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD app/build/libs/app.jar nakadi.jar
COPY app/api/nakadi-event-bus-api.yaml api/nakadi-event-bus-api.yaml
COPY app/api/nakadi-event-bus-api.yaml /zalando-apis/nakadi-event-bus-api.yaml


EXPOSE 8080 9010

ENTRYPOINT exec java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.security.egd=file:/dev/./urandom -Dspring.jdbc.getParameterType.ignore=true -jar nakadi.jar
