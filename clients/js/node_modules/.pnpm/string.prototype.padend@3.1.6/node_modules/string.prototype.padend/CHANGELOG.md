# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v3.1.6](https://github.com/es-shims/String.prototype.padEnd/compare/v3.1.5...v3.1.6) - 2024-03-21

### Commits

- [actions] use reusable workflows [`54902fb`](https://github.com/es-shims/String.prototype.padEnd/commit/54902fba3db6885780d0467c50026c4fb3bb0294)
- [Deps] update `call-bind`, `define-properties`, `es-abstract` [`b545f14`](https://github.com/es-shims/String.prototype.padEnd/commit/b545f14b4d363302dfdc75f1c1886ac07f61855d)
- [Refactor] use `es-object-atoms` where possible [`eb54e52`](https://github.com/es-shims/String.prototype.padEnd/commit/eb54e523dfbbd34c89c35905563c49067353f6bd)
- [Dev Deps] update `aud`, `npmignore`, `tape` [`b1398f3`](https://github.com/es-shims/String.prototype.padEnd/commit/b1398f30cad11a230b58c6ac1a93b994fb96121b)
- [Tests] use `call-bind` instead of `function-bind` [`3bae558`](https://github.com/es-shims/String.prototype.padEnd/commit/3bae558871aa3b34a6dc7a40963b7d07a6976825)

## [v3.1.5](https://github.com/es-shims/String.prototype.padEnd/compare/v3.1.4...v3.1.5) - 2023-09-04

### Commits

- [Deps] update `define-properties`, `es-abstract` [`b5aa85c`](https://github.com/es-shims/String.prototype.padEnd/commit/b5aa85c9c212d293b6881d6d227af9f23a1ed6c1)
- [Dev Deps] update `@es-shims/api`, `@ljharb/eslint-config`, `aud`, `tape` [`bdce52b`](https://github.com/es-shims/String.prototype.padEnd/commit/bdce52b243fa2978cc01e61b9c93e85a4d659230)

## [v3.1.4](https://github.com/es-shims/String.prototype.padEnd/compare/v3.1.3...v3.1.4) - 2022-11-07

### Commits

- [actions] reuse common workflows [`1599a3a`](https://github.com/es-shims/String.prototype.padEnd/commit/1599a3af34b28f23014c96f4d30c2ce95931b151)
- [meta] use `npmignore` to autogenerate an npmignore file [`626d38c`](https://github.com/es-shims/String.prototype.padEnd/commit/626d38ce72992fe2d3f08f31fae71b6a2a1fb020)
- [Dev Deps] update `eslint`, `@ljharb/eslint-config`, `@es-shims/api`, `safe-publish-latest`, `tape` [`9aa073a`](https://github.com/es-shims/String.prototype.padEnd/commit/9aa073a07f12e026789146dac55be7efa1bba1c4)
- [meta] add `auto-changelog` [`e48bc74`](https://github.com/es-shims/String.prototype.padEnd/commit/e48bc7404f1db9c572b7a4bcf571ce2e923b01b8)
- [Deps] update `define-properties`, `es-abstract` [`7113258`](https://github.com/es-shims/String.prototype.padEnd/commit/7113258f12294af629dd3968a5ea509dd881ba2e)
- [Dev Deps] update `eslint`, `@ljharb/eslint-config`, `aud`, `functions-have-names`, `tape` [`800dfc3`](https://github.com/es-shims/String.prototype.padEnd/commit/800dfc3bb40b4be12e0c221b9e606bad8f1d4006)
- [actions] update rebase action to use reusable workflow [`a3f9ddb`](https://github.com/es-shims/String.prototype.padEnd/commit/a3f9ddb4b25b55a7950ba3bc6a718bfab6eb7160)
- [actions] update codecov uploader [`6d2290f`](https://github.com/es-shims/String.prototype.padEnd/commit/6d2290fd32c506d6b49c37d7f110600ee4b8ef1b)

<!-- auto-changelog-above -->

3.1.3 / 2021-10-04
=================
  * [Robustness] remove runtime `.push` call
  * [readme] add github actions/codecov badges
  * [Deps] update `es-abstract`
  * [meta] use `prepublishOnly` script for npm 7+
  * [Dev Deps] update `eslint`, `@ljharb/eslint-config`, `@es-shims/api`, `aud`, `tape`
  * [actions] update workflows
  * [actions] use `node/install` instead of `node/run`; use `codecov` action

3.1.2 / 2021-02-20
=================
  * [meta] do not publish github action workflow files
  * [Deps] update `call-bind`, `es-abstract`
  * [Dev Deps] update `eslint`, `@ljharb/eslint-config`, `aud`, `functions-have-names`, `has-strict-mode`, `tape`
  * [actions] update workflows
  * [Tests] increase coverage

3.1.1 / 2020-11-21
=================
  * [Deps] update `es-abstract`; use `call-bind` where applicable
  * [Dev Deps] update `eslint`, `@ljharb/eslint-config`, `functions-have-names`, `tape`; add `aud`, `safe-publish-latest
  * [meta] gitignore nyc output
  * [actions] add "Allow Edits" workflow
  * [actions] switch Automatic Rebase workflow to `pull_request_target` event
  * [Tests] migrate tests to Github Actions
  * [Tests] run `nyc` on all tests
  * [Tests] add `implementation` test; run `es-shim-api` in postlint; use `tape` runner

3.1.0 / 2019-12-14
=================
  * [New] add `auto` entry point
  * [Refactor] use split-up `es-abstract` (77% bundle size decrease)
  * [readme] remove testling
  * [readme] Stage 4
  * [Deps] update `define-properties`, `es-abstract`, `function-bind`
  * [Dev Deps] update `eslint`, `@ljharb/eslint-config`, `covert`, `tape`, `@es-shims/api`; use `functions-have-names`
  * [meta] add `funding` field
  * [meta] Only apps should have lockfiles
  * [Tests] use shared travis-ci configs
  * [Tests] use `npx aud` instead of `nsp` or `npm audit` with hoops
  * [Tests] remove `jscs`
  * [actions] add automatic rebasing / merge commit blocking

3.0.0 / 2015-11-17
=================
  * Renamed to `padStart`/`padEnd` per November 2015 TC39 meeting.

2.0.0 / 2015-09-25
=================
  * Implement the [es-shim API](es-shims/api).
  * [Tests] up to `io.js` `v3.3`, `node` `v4.1`
  * [Deps] update `es-abstract`
  * [Dev Deps] Update `tape`, `jscs`, `eslint`, `@ljharb/eslint-config`, `nsp`
  * [Refactor] Remove redundant `max` operation, per https://github.com/ljharb/proposal-string-pad-left-right/pull/2

1.0.0 / 2015-07-30
=================
  * v1.0.0
