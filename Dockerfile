FROM registry.opensource.zalan.do/stups/openjdk:8u91-b14-1-22

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD build/libs/nakadi.jar nakadi.jar
ADD plugins plugins/
ADD scm-source.json /scm-source.json

EXPOSE 8080

# run the server when a container based on this image is being rund
ENTRYPOINT java $JAVA_OPTS -Dloader.path=/plugins -Djava.security.egd=file:/dev/./urandom -jar nakadi.jar