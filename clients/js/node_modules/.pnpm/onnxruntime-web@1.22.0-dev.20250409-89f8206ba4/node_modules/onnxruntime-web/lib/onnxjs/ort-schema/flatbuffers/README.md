# ORT Format File

This directory contains [the generated ts file](ort-generated.ts) necessary to support the ORT file format. The file is generated from [the ORT file format schema](https://github.com/microsoft/onnxruntime/blob/main/onnxruntime/core/flatbuffers/schema/ort.fbs). Please do not directly modify [the generated ts header file](ort-generated.ts).

[The ORT file format schema](https://github.com/microsoft/onnxruntime/blob/main/onnxruntime/core/flatbuffers/schema/ort.fbs) uses [FlatBuffers](https://github.com/google/flatbuffers) serialization library. To update [its generated ts file](ort-generated.ts),

1. Download or locate the [ort.fbs](https://github.com/microsoft/onnxruntime/blob/main/onnxruntime/core/flatbuffers/schema/ort.fbs) file.
2. Download FlatBuffers compiler: Download the latest flatc tool (Windows.flatc.binary.zip) from [Flatbuffers Release Page](https://github.com/google/flatbuffers/releases). Unzip and run

`> flatc.exe --ts ort.fbs`

copy the generated folder `onnxruntime` to `js/web/lib/onnxjs/ort-schema/flatbuffers/` directory.

Update ort-generated.ts to re-export from the generated file fbs.ts under the onnxruntime directory.
