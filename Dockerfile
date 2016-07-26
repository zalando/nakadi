FROM registry.opensource.zalan.do/stups/openjdk:8u91-b14-1-22

MAINTAINER Team Aruha, team-aruha@zalando.de

WORKDIR /usr/bin/nakadi
ADD build/distributions/nakadi.tgz /usr/bin/

EXPOSE 8080

# run the server when a container based on this image is being run
ENTRYPOINT bin/run.sh

