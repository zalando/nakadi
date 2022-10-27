## gatling project build as uberjar
FROM gradle:5.3.1-jdk11-slim AS java-build

WORKDIR /app
ADD src src
ADD build.gradle build.gradle

COPY --chown=gradle:gradle . /app
RUN gradle build --no-daemon
## java (run)
FROM adoptopenjdk/openjdk11:x86_64-alpine-jre-11.0.3_7 AS java-run

ENV GLIBC_VER=2.31-r0

## AWS CLI installation
RUN apk --no-cache add \
        binutils \
        curl \
    && curl -sL https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub -o /etc/apk/keys/sgerrand.rsa.pub \
    && curl -sLO https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VER}/glibc-${GLIBC_VER}.apk \
    && curl -sLO https://github.com/sgerrand/alpine-pkg-glibc/releases/download/${GLIBC_VER}/glibc-bin-${GLIBC_VER}.apk \
    && apk add --no-cache \
        glibc-${GLIBC_VER}.apk \
        glibc-bin-${GLIBC_VER}.apk \
    && curl -sL https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip \
    && unzip awscliv2.zip \
    && aws/install \
    && rm -rf \
        awscliv2.zip \
        aws \
        /usr/local/aws-cli/v2/*/dist/aws_completer \
        /usr/local/aws-cli/v2/*/dist/awscli/data/ac.index \
        /usr/local/aws-cli/v2/*/dist/awscli/examples \
    && apk --no-cache del \
        binutils \
        curl \
    && rm glibc-${GLIBC_VER}.apk \
    && rm glibc-bin-${GLIBC_VER}.apk \
    && rm -rf /var/cache/apk/*

RUN apk add docker

RUN aws --version && docker --version

ENV APP_HOME /app

RUN mkdir -m 0755 -p ${APP_HOME}/bin

COPY --from=java-build /app/build/libs/api-perftest-1.1.3-all.jar ${APP_HOME}/bin/api-perftest-1.1.3-all.jar

COPY docker-entrypoint.sh /

RUN chmod +x /docker-entrypoint.sh

RUN addgroup -S gatling && adduser -S gatling -G gatling

RUN chown -R gatling:gatling ${APP_HOME}

RUN chown gatling:gatling /docker-entrypoint.sh

USER gatling

WORKDIR ${APP_HOME}

ENV JAVA_TOOL_OPTIONS "-Denv=perf"

#Perf test execution and S3 report creation#
CMD ["sh","/docker-entrypoint.sh"]
