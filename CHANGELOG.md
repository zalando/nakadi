## [1.1.16](https://github.com/wiley/nakadi/compare/v1.1.15...v1.1.16) (2022-12-09)

### Bug Fixes

* test ([21f2b0f](https://github.com/wiley/nakadi/commit/21f2b0fad57f5affa3bf2b5c1d24924851321cb7))
* test ([592aca5](https://github.com/wiley/nakadi/commit/592aca58f025af448dbb3a5d08835b85dcdf2beb))

## [1.1.15]

## [1.1.14](https://github.com/wiley/nakadi/compare/v1.1.13...v1.1.14) (2022-11-24)
### Bug Fixes

* disable inti db execution when postgres container startup in linux machines because the liquibase is also executing afterwards ([0963ff6](https://github.com/wiley/nakadi/commit/0963ff602e89354b7651757fb4612770de0e0903))
* replace eventhub roles with epdcs roles since there is a access denied issue ([691e0f8](https://github.com/wiley/nakadi/commit/691e0f8aa9dc54a336f2691f0d5df45a700a1441))

## [1.1.13](https://github.com/wiley/nakadi/compare/v1.1.12...v1.1.13) (2022-11-24)


### Bug Fixes

* replace epdcs roles with eventhub roles ([33d2771](https://github.com/wiley/nakadi/commit/33d27714bc30145160e3a21fd8d9e291d6b2495c))

## [1.1.12](https://github.com/wiley/nakadi/compare/v1.1.11...v1.1.12) (2022-11-21)


### Bug Fixes

* revert eventhub role arn to epdcs role since this is currently deployed under epdcs syscode ([d2b7286](https://github.com/wiley/nakadi/commit/d2b7286ca864f9aabf10426a1ee3168635e7ce09))

## [1.1.11](https://github.com/wiley/nakadi/compare/v1.1.10...v1.1.11) (2022-11-17)


### Bug Fixes

* add image ([e8d002e](https://github.com/wiley/nakadi/commit/e8d002e6db6121ddd8423e1e861492ef78889ffb))
* check build issue ([687f520](https://github.com/wiley/nakadi/commit/687f5201b5dbd31391c54f779b401b10b37c6634))
* config new kafka cluster and dbs ([6de69d4](https://github.com/wiley/nakadi/commit/6de69d480eb6072e5aae2639a8290eb06b6037ff))
* configure eventhub role and db sceret ([0abc074](https://github.com/wiley/nakadi/commit/0abc074475967df89ccddf18815c4678fc2f32f6))
* remove db password from automation test configs and get it as ci a variable ([3d56d60](https://github.com/wiley/nakadi/commit/3d56d602cd36b0c9e6aa55d10655c0f277f6db4c))
* remove perf module ([ef7d4bf](https://github.com/wiley/nakadi/commit/ef7d4bf16002d04f9aaa86914f5c223802c76eca))
* remove ref latest_image ([85e0bdb](https://github.com/wiley/nakadi/commit/85e0bdb7bab7eb7fb1335b16905a70a1fd6c4b9f))
* set kafka external port in local configs ([d8d7e2f](https://github.com/wiley/nakadi/commit/d8d7e2fa887b8e499d4ccece85b6303ee988ab08))
* test ([8b477b8](https://github.com/wiley/nakadi/commit/8b477b82a40ae5b2b0eb2e31e50208e6fc1185bd))
* update perf module ([28088f4](https://github.com/wiley/nakadi/commit/28088f43cc02ab0034e98847d735694ff640019d))
* update reference conf ([6438bfb](https://github.com/wiley/nakadi/commit/6438bfbf39fd3c385cc07c76550dbc3e5840a104))

## [1.1.10](https://github.com/wiley/nakadi/compare/v1.1.9...v1.1.10) (2022-10-19)

### Bug Fixes

* test commit ([0536087](https://github.com/wiley/nakadi/commit/0536087ecd91418cf84e9b32e4c16b8d2e704aa6))

## [1.1.9](https://github.com/wiley/nakadi/compare/v1.1.8...v1.1.9) (2022-10-05)

### Bug Fixes

* get db password from secretKey test commit ([1574284](https://github.com/wiley/nakadi/commit/1574284cb2c12b996c11c33bf9b0fe6bda5a8bbc))

## [1.1.8](https://github.com/wiley/nakadi/compare/v1.1.7...v1.1.8) (2022-10-04)


### Bug Fixes

* change timeout on tests ([8d24edf](https://github.com/wiley/nakadi/commit/8d24edf4d0b5bbdd6d7613d34677c64a422c8fe0))

## [1.1.7](https://github.com/wiley/nakadi/compare/v1.1.6...v1.1.7) (2022-10-03)


### Bug Fixes

* increase test timeout ([cd50e9b](https://github.com/wiley/nakadi/commit/cd50e9b7beefdc00cac152dfaa7d65edfe1a0bb4))
* shouldChangeDefaultStorageWhenRequested automation test ([aa46445](https://github.com/wiley/nakadi/commit/aa4644503980b24c1ffd9147bd93c1c1aad52970))

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
