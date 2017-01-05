FROM registry.opensource.zalan.do/stups/openjdk:8-54

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /
ADD build/libs/nakadi.jar nakadi.jar

EXPOSE 8080

ENTRYPOINT java -Djava.security.egd=file:/dev/./urandom -jar nakadi.jar
