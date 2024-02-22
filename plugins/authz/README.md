# nakadi-plugin-authz

Zalando-specific, per-resource authorization plugin for 
[Nakadi](https://github.com/zalando/nakadi).

This plugin implements the `AuthorizationService` and `AuthorizationServiceFactory` classes from 
[nakadi-plugin-api](https://github.com/zalando-incubator/nakadi-plugin-api).

It provides a Policy Decision Point, where Nakadi can find out whether a subject is allowed to 
perform a given operation on a resource. It always provides (part of) a Policy Information Point, 
where Nakadi can find out whether a given subject actually exists.

## Policy Decision Point

The policy decision point uses Spring's security context to extract the subject's uid and realm. It ignores the 
`Subject` interface provided in `isAuthorized(..)`. Access is authorized iff the subject is part of the resource's 
authorized subjects for the operation, or if the authorized subjects includes a wildcard.

## Policy Information Point

The policy information point queries Zalando's authentication endpoint (for employees or for services, as appropriate)
to find out whether an authorization attribute actually exists.

## Build

Before building, make sure you define the following two variables, either in `gradle.properties` (not a good idea), or 
in `~/.gradle/gradle.properties` (much better):
* nexusUsername
* nexusPassword

To build the plugin, run `gradle build`. The build includes a pass with checkstyle 
(same configuration as Nakadi), 
and runs all unit tests. It produces a jar file in `build/libs`.

By default, the build will include the version number (in `gradle.properties`), followed by `-SNAPSHOT`.
You can include the `release` task to substitute `-SNAPSHOT` with `-RELEASE`: `gradle build -q release`.
To get a jar without a version number in its name, use the `noversion` task: `gradle build -q noversion`.

## Configuration

This plugin provides the following configuration items:
* `nakadi.plugins.authz.usersRealm`: the users (employees) realm, as returned from Zalando's authentication endpoint. Currently "/employees".
* `nakadi.plugins.authz.servicesRealm`: the services realm, as returned by Zalando's authentication endpoint. Currently "/services"
* `nakadi.plugins.authz.usersType`: the data type for users in the authorization attribute. Currently "user"
* `nakadi.plugins.authz.servicesType`: the data type for services in the authorization attribute. Currently "service"
* `nakadi.plugins.authz.usersEndpoint`: the endpoint URL for user authentication. Currently https://users.auth.zalando.com/api/employees/
* `nakadi.plugins.authz.servicesEndpoint`: the endpoint URL for service authentication. Currently https://services.auth.zalando.com/api/services/
* `nakadi.plugins.authz.authTokenUri`: the URI for getting a token, necessary to query the users and services endpoints
* `nakadi.plugins.authz.tokenId`: nakadi token id
* `nakadi.plugins.authz.validatePrincipalExists`: enables checking the existence of users and applications attributes

## CI/CD

This plugin is automatically built by [CDP](https://delivery.cloud.zalando.com/ui). A build is triggered when code is 
pushed to a PR (or when a PR is created), and when a PR is merged.

If code was pushed to a PR, after the build succeeds, CDP will upload a snapshot to 
[maven.zalando.net](https://maven.zalando.net) (snapshot repository). Snapshots can be overwritten, so it is fine to 
trigger multiple builds.

If code was merged, after the build succeeds, CDP will upload a release to [maven.zalando.net](https://maven.zalando.net) 
(releases repository). Releases cannot be overwritten, so make sure to update the version number before merging!
