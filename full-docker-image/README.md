# Building the full Docker image of Nakadi

## Idea

Use this approach to build a Nakadi image which incorporates all requirements in one Docker image. This can be useful
when you want to start up a single image without dependencies to other components. You could for example integrate this
image into your integration tests for Nakadi.


## How to build the Docker image

Go to the project root and call `./gradlew buildFullDockerImage`. By default this shall create a Docker image with the
name `aruha/full-nakadi:AUTOBUILD`. You can change this by specifying one or more of the following parameters (being set
by adding `-D<parameter>=<value>` to the `gradlew` call):

- fullDockerImageName
- fullDockerImageVersion

The docker image name will be like `$fullDockerImageName:$fullDockerImageVersion`.

## Run the Docker image

To run the Docker image use the following command:

`docker run -e "NAKADI_OAUTH2_MODE=OFF" -e "NAKADI_FEATURETOGGLE_ENABLEALL=true" -p 8080:8080 -i -t full-nakadi`

This will expose NAKADI to port 8080 on your local machine (or on your docker machine if using a Mac or PC).
