FROM zalando/openjdk:8u66-b17-1-3

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD build/libs/nakadi.jar nakadi.jar

EXPOSE 8080

# run the server when a container based on this image is being run
ENTRYPOINT java -jar -Djava.security.egd=file:/dev/./urandom nakadi.jar

