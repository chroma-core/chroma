# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [1.1.6](https://github.com/mongodb/js-bson/compare/v1.1.5...v1.1.6) (2021-03-16)


### Bug Fixes

* Throw error on bigint usage and add helpers to Long ([#426](https://github.com/mongodb/js-bson/issues/426)) ([375f368](https://github.com/mongodb/js-bson/commit/375f368738807f2d41c7751e618fd09c8a1b94c9))

### [1.1.5](https://github.com/mongodb/js-bson/compare/v1.1.4...v1.1.5) (2020-08-10)


### Bug Fixes

* **object-id:** harden the duck-typing ([b526145](https://github.com/mongodb/js-bson/commit/b5261450c3bc4abb2e2fb19b5b1a7aba27982d44))

<a name="1.1.3"></a>
## [1.1.3](https://github.com/mongodb/js-bson/compare/v1.1.2...v1.1.3) (2019-11-09)

Reverts 1.1.2

<a name="1.1.2"></a>
## [1.1.2](https://github.com/mongodb/js-bson/compare/v1.1.1...v1.1.2) (2019-11-08)


### Bug Fixes

* **_bsontype:** only check bsontype if it is a prototype member. ([dd8a349](https://github.com/mongodb/js-bson/commit/dd8a349))



<a name="1.1.1"></a>
## [1.1.1](https://github.com/mongodb/js-bson/compare/v1.1.0...v1.1.1) (2019-03-08)


### Bug Fixes

* **object-id:** support 4.x->1.x interop for MinKey and ObjectId ([53419a5](https://github.com/mongodb/js-bson/commit/53419a5))


### Features

* replace new Buffer with modern versions ([24aefba](https://github.com/mongodb/js-bson/commit/24aefba))



<a name="1.1.0"></a>
# [1.1.0](https://github.com/mongodb/js-bson/compare/v1.0.9...v1.1.0) (2018-08-13)


### Bug Fixes

* **serializer:** do not use checkKeys for $clusterTime ([573e141](https://github.com/mongodb/js-bson/commit/573e141))



<a name="1.0.9"></a>
## [1.0.9](https://github.com/mongodb/js-bson/compare/v1.0.8...v1.0.9) (2018-06-07)


### Bug Fixes

* **serializer:** remove use of `const` ([5feb12f](https://github.com/mongodb/js-bson/commit/5feb12f))



<a name="1.0.7"></a>
## [1.0.7](https://github.com/mongodb/js-bson/compare/v1.0.6...v1.0.7) (2018-06-06)


### Bug Fixes

* **binary:** add type checking for buffer ([26b05b5](https://github.com/mongodb/js-bson/commit/26b05b5))
* **bson:** fix custom inspect property ([080323b](https://github.com/mongodb/js-bson/commit/080323b))
* **readme:** clarify documentation about deserialize methods ([20f764c](https://github.com/mongodb/js-bson/commit/20f764c))
* **serialization:** normalize function stringification ([1320c10](https://github.com/mongodb/js-bson/commit/1320c10))



<a name="1.0.6"></a>
## [1.0.6](https://github.com/mongodb/js-bson/compare/v1.0.5...v1.0.6) (2018-03-12)


### Features

* **serialization:** support arbitrary sizes for the internal serialization buffer ([abe97bc](https://github.com/mongodb/js-bson/commit/abe97bc))



<a name="1.0.5"></a>
## 1.0.5 (2018-02-26)


### Bug Fixes

* **decimal128:** add basic guard against REDOS attacks ([bd61c45](https://github.com/mongodb/js-bson/commit/bd61c45))
* **objectid:** if pid is 1, use random value ([e188ae6](https://github.com/mongodb/js-bson/commit/e188ae6))



1.0.4 2016-01-11
----------------
- #204 remove Buffer.from as it's partially broken in early 4.x.x. series of node releases.

1.0.3 2016-01-03
----------------
- Fixed toString for ObjectId so it will work with inspect.

1.0.2 2016-01-02
----------------
- Minor optimizations for ObjectID to use Buffer.from where available.

1.0.1 2016-12-06
----------------
- Reverse behavior for undefined to be serialized as NULL. MongoDB 3.4 does not allow for undefined comparisons.

1.0.0 2016-12-06
----------------
- Introduced new BSON API and documentation.

0.5.7 2016-11-18
-----------------
- NODE-848 BSON Regex flags must be alphabetically ordered.

0.5.6 2016-10-19
-----------------
- NODE-833, Detects cyclic dependencies in documents and throws error if one is found.
- Fix(deserializer): corrected the check for (size + index) comparison… (Issue #195, https://github.com/JoelParke).

0.5.5 2016-09-15
-----------------
- Added DBPointer up conversion to DBRef

0.5.4 2016-08-23
-----------------
- Added promoteValues flag (default to true) allowing user to specify if deserialization should be into wrapper classes only.

0.5.3 2016-07-11
-----------------
- Throw error if ObjectId is not a string or a buffer.

0.5.2 2016-07-11
-----------------
- All values encoded big-endian style for ObjectId.

0.5.1 2016-07-11
-----------------
- Fixed encoding/decoding issue in ObjectId timestamp generation.
- Removed BinaryParser dependency from the serializer/deserializer.

0.5.0 2016-07-05
-----------------
- Added Decimal128 type and extended test suite to include entire bson corpus.

0.4.23 2016-04-08
-----------------
- Allow for proper detection of ObjectId or objects that look like ObjectId, improving compatibility across third party libraries.
- Remove one package from dependency due to having been pulled from NPM.

0.4.22 2016-03-04
-----------------
- Fix "TypeError: data.copy is not a function" in Electron (Issue #170, https://github.com/kangas).
- Fixed issue with undefined type on deserializing.

0.4.21 2016-01-12
-----------------
- Minor optimizations to avoid non needed object creation.

0.4.20 2015-10-15
-----------------
- Added bower file to repository.
- Fixed browser pid sometimes set greater than 0xFFFF on browsers (Issue #155, https://github.com/rahatarmanahmed)

0.4.19 2015-10-15
-----------------
- Remove all support for bson-ext.

0.4.18 2015-10-15
-----------------
- ObjectID equality check should return boolean instead of throwing exception for invalid oid string #139
- add option for deserializing binary into Buffer object #116

0.4.17 2015-10-15
-----------------
- Validate regexp string for null bytes and throw if there is one.

0.4.16 2015-10-07
-----------------
- Fixed issue with return statement in Map.js.

0.4.15 2015-10-06
-----------------
- Exposed Map correctly via index.js file.

0.4.14 2015-10-06
-----------------
- Exposed Map correctly via bson.js file.

0.4.13 2015-10-06
-----------------
- Added ES6 Map type serialization as well as a polyfill for ES5.

0.4.12 2015-09-18
-----------------
- Made ignore undefined an optional parameter.

0.4.11 2015-08-06
-----------------
- Minor fix for invalid key checking.

0.4.10 2015-08-06
-----------------
- NODE-38 Added new BSONRegExp type to allow direct serialization to MongoDB type.
- Some performance improvements by in lining code.

0.4.9 2015-08-06
----------------
- Undefined fields are omitted from serialization in objects.

0.4.8 2015-07-14
----------------
- Fixed size validation to ensure we can deserialize from dumped files.

0.4.7 2015-06-26
----------------
- Added ability to instruct deserializer to return raw BSON buffers for named array fields.
- Minor deserialization optimization by moving inlined function out.

0.4.6 2015-06-17
----------------
- Fixed serializeWithBufferAndIndex bug.

0.4.5 2015-06-17
----------------
- Removed any references to the shared buffer to avoid non GC collectible bson instances.

0.4.4 2015-06-17
----------------
- Fixed rethrowing of error when not RangeError.

0.4.3 2015-06-17
----------------
- Start buffer at 64K and double as needed, meaning we keep a low memory profile until needed.

0.4.2 2015-06-16
----------------
- More fixes for corrupt Bson

0.4.1 2015-06-16
----------------
- More fixes for corrupt Bson

0.4.0 2015-06-16
----------------
- New JS serializer serializing into a single buffer then copying out the new buffer. Performance is similar to current C++ parser.
- Removed bson-ext extension dependency for now.

0.3.2 2015-03-27
----------------
- Removed node-gyp from install script in package.json.

0.3.1 2015-03-27
----------------
- Return pure js version on native() call if failed to initialize.

0.3.0 2015-03-26
----------------
- Pulled out all C++ code into bson-ext and made it an optional dependency.

0.2.21 2015-03-21
-----------------
- Updated Nan to 1.7.0 to support io.js and node 0.12.0

0.2.19 2015-02-16
-----------------
- Updated Nan to 1.6.2 to support io.js and node 0.12.0

0.2.18 2015-01-20
-----------------
- Updated Nan to 1.5.1 to support io.js

0.2.16 2014-12-17
-----------------
- Made pid cycle on 0xffff to avoid weird overflows on creation of ObjectID's

0.2.12 2014-08-24
-----------------
- Fixes for fortify review of c++ extension
- toBSON correctly allows returns of non objects

0.2.3 2013-10-01
----------------
- Drying of ObjectId code for generation of id (Issue #54, https://github.com/moredip)
- Fixed issue where corrupt CString's could cause endless loop
- Support for Node 0.11.X > (Issue #49, https://github.com/kkoopa)

0.1.4 2012-09-25
----------------
- Added precompiled c++ native extensions for win32 ia32 and x64
