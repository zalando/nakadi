FROM container-registry.zalando.net/library/eclipse-temurin-11-jdk:latest

MAINTAINER Team Aruha, team-aruha@zalando.de

EXPOSE 8080

WORKDIR /

ENV JOLOKIA_VERSION="1.6.2"

RUN apt-get update && \
    apt-get install -y wget && \
    \
    wget -O "/jolokia-jvm-agent.jar" "http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/${JOLOKIA_VERSION}/jolokia-jvm-${JOLOKIA_VERSION}-agent.jar" && \
    chmod 744 /jolokia-jvm-agent.jar && \
    echo "b2ede18f41ec70fcd5fc94a6e2a23c0d0983677e /jolokia-jvm-agent.jar" > /jolokia-jvm-agent.jar.sha1 && \
    sha1sum --check /jolokia-jvm-agent.jar.sha1 && \
    rm -f /jolokia-jvm-agent.jar.sha1 && \
    \
    apt-get -y autoremove && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY app/build/libs/app.jar nakadi.jar
COPY plugins/authz/build/libs/nakadi-plugin-authz.jar plugins/lightstep/build/libs/nakadi-lightstep.jar plugins/

COPY app/api/nakadi-event-bus-api.yaml api/nakadi-event-bus-api.yaml
COPY app/api/nakadi-event-bus-api.yaml /zalando-apis/nakadi-event-bus-api.yaml

# to make the plugins load, add to JAVA_OPTS: -Dloader.path=/plugins

ENTRYPOINT exec java $JAVA_OPTS \
    -javaagent:/jolokia-jvm-agent.jar=host=0.0.0.0 \
    -Djava.security.egd=file:/dev/./urandom \
    -Dspring.jdbc.getParameterType.ignore=true \
    -jar nakadi.jar
