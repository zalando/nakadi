FROM zalando/openjdk:8u66-b17-1-3

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD build/libs/nakadi.jar nakadi.jar
ADD scm-source.json /scm-source.json

EXPOSE 8080

# run the server when a container based on this image is being run
ENTRYPOINT java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar nakadi.jar

