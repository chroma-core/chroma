"use strict";
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const child_process_1 = require("child_process");
const fs = __importStar(require("fs-extra"));
const minimist_1 = __importDefault(require("minimist"));
const os = __importStar(require("os"));
const path = __importStar(require("path"));
// command line flags
const buildArgs = (0, minimist_1.default)(process.argv.slice(2));
// --config=Debug|Release|RelWithDebInfo
const CONFIG = buildArgs.config || (os.platform() === 'win32' ? 'RelWithDebInfo' : 'Release');
if (CONFIG !== 'Debug' && CONFIG !== 'Release' && CONFIG !== 'RelWithDebInfo') {
    throw new Error(`unrecognized config: ${CONFIG}`);
}
// --arch=x64|ia32|arm64|arm
const ARCH = buildArgs.arch || os.arch();
if (ARCH !== 'x64' && ARCH !== 'ia32' && ARCH !== 'arm64' && ARCH !== 'arm') {
    throw new Error(`unrecognized architecture: ${ARCH}`);
}
// --onnxruntime-build-dir=
const ONNXRUNTIME_BUILD_DIR = buildArgs['onnxruntime-build-dir'];
// --onnxruntime-generator=
const ONNXRUNTIME_GENERATOR = buildArgs['onnxruntime-generator'];
// --rebuild
const REBUILD = !!buildArgs.rebuild;
// --use_dml
const USE_DML = !!buildArgs.use_dml;
// --use_webgpu
const USE_WEBGPU = !!buildArgs.use_webgpu;
// --use_cuda
const USE_CUDA = !!buildArgs.use_cuda;
// --use_tensorrt
const USE_TENSORRT = !!buildArgs.use_tensorrt;
// --use_coreml
const USE_COREML = !!buildArgs.use_coreml;
// --use_qnn
const USE_QNN = !!buildArgs.use_qnn;
// --dll_deps=
const DLL_DEPS = buildArgs.dll_deps;
// build path
const ROOT_FOLDER = path.join(__dirname, '..');
const BIN_FOLDER = path.join(ROOT_FOLDER, 'bin');
const BUILD_FOLDER = path.join(ROOT_FOLDER, 'build');
// if rebuild, clean up the dist folders
if (REBUILD) {
    fs.removeSync(BIN_FOLDER);
    fs.removeSync(BUILD_FOLDER);
}
const args = [
    'cmake-js',
    REBUILD ? 'reconfigure' : 'configure',
    `--arch=${ARCH}`,
    '--CDnapi_build_version=6',
    `--CDCMAKE_BUILD_TYPE=${CONFIG}`,
];
if (ONNXRUNTIME_BUILD_DIR && typeof ONNXRUNTIME_BUILD_DIR === 'string') {
    args.push(`--CDONNXRUNTIME_BUILD_DIR=${ONNXRUNTIME_BUILD_DIR}`);
}
if (ONNXRUNTIME_GENERATOR && typeof ONNXRUNTIME_GENERATOR === 'string') {
    args.push(`--CDONNXRUNTIME_GENERATOR=${ONNXRUNTIME_GENERATOR}`);
}
if (USE_DML) {
    args.push('--CDUSE_DML=ON');
}
if (USE_WEBGPU) {
    args.push('--CDUSE_WEBGPU=ON');
}
if (USE_CUDA) {
    args.push('--CDUSE_CUDA=ON');
}
if (USE_TENSORRT) {
    args.push('--CDUSE_TENSORRT=ON');
}
if (USE_COREML) {
    args.push('--CDUSE_COREML=ON');
}
if (USE_QNN) {
    args.push('--CDUSE_QNN=ON');
}
if (DLL_DEPS) {
    args.push(`--CDORT_NODEJS_DLL_DEPS=${DLL_DEPS}`);
}
// set CMAKE_OSX_ARCHITECTURES for macOS build
if (os.platform() === 'darwin') {
    if (ARCH === 'x64') {
        args.push('--CDCMAKE_OSX_ARCHITECTURES=x86_64');
    }
    else if (ARCH === 'arm64') {
        args.push('--CDCMAKE_OSX_ARCHITECTURES=arm64');
    }
    else {
        throw new Error(`architecture not supported for macOS build: ${ARCH}`);
    }
}
// In Windows, "npx cmake-js configure" uses a powershell script to detect the Visual Studio installation.
// The script uses the environment variable LIB. If an invalid path is specified in LIB, the script will fail.
// So we override the LIB environment variable to remove invalid paths.
const envOverride = os.platform() === 'win32' && process.env.LIB
    ? { ...process.env, LIB: process.env.LIB.split(';').filter(fs.existsSync).join(';') }
    : process.env;
// launch cmake-js configure
const procCmakejs = (0, child_process_1.spawnSync)('npx', args, { shell: true, stdio: 'inherit', cwd: ROOT_FOLDER, env: envOverride });
if (procCmakejs.status !== 0) {
    if (procCmakejs.error) {
        console.error(procCmakejs.error);
    }
    process.exit(procCmakejs.status === null ? undefined : procCmakejs.status);
}
// launch cmake to build
const procCmake = (0, child_process_1.spawnSync)('cmake', ['--build', '.', '--config', CONFIG], {
    shell: true,
    stdio: 'inherit',
    cwd: BUILD_FOLDER,
});
if (procCmake.status !== 0) {
    if (procCmake.error) {
        console.error(procCmake.error);
    }
    process.exit(procCmake.status === null ? undefined : procCmake.status);
}
