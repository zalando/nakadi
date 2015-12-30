[![Build Status](https://travis-ci.org/zalando/nakadi.svg)](https://travis-ci.org/zalando/nakadi)
[![codecov.io](https://codecov.io/github/zalando/nakadi/coverage.svg?branch=nakadi-jvm)](https://codecov.io/github/zalando/nakadi?branch=nakadi-jvm)

Simple Nakadi startup:

gradle run

Full development pipeline:
build -> ut/it tests (depends on access to a kafka backend) -> docker (builds docker image) -> api-tests (runs tests against the docker image)


