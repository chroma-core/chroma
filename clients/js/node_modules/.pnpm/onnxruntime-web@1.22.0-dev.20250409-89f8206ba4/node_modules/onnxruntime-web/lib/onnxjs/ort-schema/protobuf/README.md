# ONNX protobuf

This directory contains generated protobuf definition for onnx:

- onnx.js
- onnx.d.ts

These files are generated from [a fork of onnx-proto](https://github.com/fs-eire/onnx-proto/tree/update-v9).

The ONNX protobuf uses protobufjs@7.2.4, which depends on long@5.2.3, the version contains 2 bugs:

- type export does not work with commonjs. described in https://github.com/dcodeIO/long.js/pull/124. added a "postinstall" script to fix.
- in the generated typescript declaration file 'onnx.d.ts', the following line:
  ```ts
  import Long = require('long');
  ```
  need to be replaced to fix type import error:
  ```ts
  import Long from 'long';
  ```
  this replacement is done and code format is also applied to file 'onnx.d.ts'.
