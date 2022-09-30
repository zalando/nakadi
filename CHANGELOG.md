## [1.1.6](https://github.com/wiley/nakadi/compare/v1.1.5...v1.1.6) (2022-09-29)


### Bug Fixes

* add perf module ([e4caad9](https://github.com/wiley/nakadi/commit/e4caad9178a375d12e715f04737b80ae532c9cbc))

## [1.1.5](https://github.com/wiley/nakadi/compare/v1.1.4...v1.1.5) (2022-09-28)


### Bug Fixes

* add iam auth dependency for acceptance test model ([2d79286](https://github.com/wiley/nakadi/commit/2d79286a7f9169732d5685904a23124594937ad8))
* check batches according to env specific batch size ([cfb4f72](https://github.com/wiley/nakadi/commit/cfb4f72d313fdb0100d4aeb21d50b1d1f6959f70))
* increase test timeout ([a9f7a5a](https://github.com/wiley/nakadi/commit/a9f7a5a1ee7efbd3eed86a67d4253092f5b895c4))

## [1.1.4](https://github.com/wiley/nakadi/compare/v1.1.3...v1.1.4) (2022-09-26)


### Bug Fixes

* add external configs to integration tests ([e91d7aa](https://github.com/wiley/nakadi/commit/e91d7aa316095e39aeaf08586bf2de25abcd9754))
* add external kafaka security configs ([56a93fe](https://github.com/wiley/nakadi/commit/56a93fea0b7d541d57a31bdad6c2f94451061da2))
* append port to url in only for local environment ([779c502](https://github.com/wiley/nakadi/commit/779c502b456b9a93af49b7a5d11d5a317d793c68))
* check null preventing nullpointer exception ([c65f5da](https://github.com/wiley/nakadi/commit/c65f5da95fa95ff228d68ce8fbe867a724223cad))
* replace hard coded kafka url with variable ([2655ad4](https://github.com/wiley/nakadi/commit/2655ad48558e048bc4a20e6e1624f6f020539c17))

## [1.1.3](https://github.com/wiley/nakadi/compare/v1.1.2...v1.1.3) (2022-09-19)


### Bug Fixes

* test untrusted builder issue attempt 3 ([090ca8c](https://github.com/wiley/nakadi/commit/090ca8c8cb81fe7434ba055fd07ffd23729e5c11))

## [1.1.2](https://github.com/wiley/nakadi/compare/v1.1.1...v1.1.2) (2022-09-19)


### Bug Fixes

* test untrusted builder issue attempt 2 ([5a230a7](https://github.com/wiley/nakadi/commit/5a230a7b40ef2d712d04ab53fc7ede064cb31a5b))

## [1.1.1](https://github.com/wiley/nakadi/compare/v1.1.0...v1.1.1) (2022-09-19)


### Bug Fixes

* test untrusted builder issue ([15f207c](https://github.com/wiley/nakadi/commit/15f207c9ac3225b94312738624b193dc71b52a1c))

# [1.1.0](https://github.com/wiley/nakadi/compare/v1.0.6...v1.1.0) (2022-09-14)


### Bug Fixes

* add prometheus metrics ([7343347](https://github.com/wiley/nakadi/commit/73433471309460d6480bfdf6165225c41387a4ea))
* enable application monitoring ([f8d1a99](https://github.com/wiley/nakadi/commit/f8d1a998ee3436f2903e082e11d7426cd75642ef))
* update gradle ([81d1a2b](https://github.com/wiley/nakadi/commit/81d1a2bac3facae398affb68d3d969adba8eeee5))
* use latest gitlab template ([425ec65](https://github.com/wiley/nakadi/commit/425ec6514e10a7f650b2d32022deee388aaf2d35))


### Features

* adding absolute path for copy task ([89f59ec](https://github.com/wiley/nakadi/commit/89f59ec1c0335c53d55bf164a3f489d7b271c8f6))
* adding gradle task to execute liquibase scripts in app ([aca7bad](https://github.com/wiley/nakadi/commit/aca7bad22fab523477aa9721f3071a500c9f3dbc))
* adding task to check root dir ([d815f06](https://github.com/wiley/nakadi/commit/d815f065a4f052b4b387edbd16fc4c36b0c96c60))
* deleted nakadi-db sql file ([6b7724c](https://github.com/wiley/nakadi/commit/6b7724cb1cdd27ced33c791ff8303e06b69b4812))

## [1.0.6](https://github.com/wiley/nakadi/compare/v1.0.5...v1.0.6) (2022-08-25)


### Bug Fixes

* revert changes on health endpoint path for livenessProbe and readinessProbe ([6b63063](https://github.com/wiley/nakadi/commit/6b630637cf3fb4175c41a163f587cb829cd470da))

## [1.0.5](https://github.com/wiley/nakadi/compare/v1.0.4...v1.0.5) (2022-08-25)


### Bug Fixes

* add health endpoint path for livenessProbe and readinessProbe ([119a8a1](https://github.com/wiley/nakadi/commit/119a8a148397f1ca0a07008d828de2cfa66e9698))
* expose qa public port setting ingressClass as nginx-external ([8bb762e](https://github.com/wiley/nakadi/commit/8bb762e0d50ea4231860a39a9d69afc19dc9bf04))

## [1.0.4](https://github.com/wiley/nakadi/compare/v1.0.3...v1.0.4) (2022-08-23)


### Bug Fixes

* remove awsProfileName tc-dev ([44093e4](https://github.com/wiley/nakadi/commit/44093e4f260486cfe2c704520fecf943ae8bf66c))

## [1.0.3](https://github.com/wiley/nakadi/compare/v1.0.2...v1.0.3) (2022-08-23)


### Bug Fixes

* add all profiles into RepositoriesConfig ([a4f299e](https://github.com/wiley/nakadi/commit/a4f299ed5660dc647e49dc491bab852c9cf9b066))
* fix qa db url ([9953c07](https://github.com/wiley/nakadi/commit/9953c074392a1775e4a9c3882782bb236bcc63c8))

## [1.0.2](https://github.com/wiley/nakadi/compare/v1.0.1...v1.0.2) (2022-08-23)


### Bug Fixes

* revert appcat changes and update systemId as other services ([46b9b23](https://github.com/wiley/nakadi/commit/46b9b238a5cee0531e694155bbb2ba2801d5df63))

## [1.0.1](https://github.com/wiley/nakadi/compare/v1.0.0...v1.0.1) (2022-08-18)


### Bug Fixes

* change test port to create new release ([f20c5e2](https://github.com/wiley/nakadi/commit/f20c5e2d9cd887f490d67810fafd0db5345092a6))
