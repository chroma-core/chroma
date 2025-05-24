// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

'use strict';

// This script is written in JavaScript. This is because it is used in "install" script in package.json, which is called
// when the package is installed either as a dependency or from "npm ci"/"npm install" without parameters. TypeScript is
// not always available.

// The purpose of this script is to download the required binaries for the platform and architecture.
// Currently, most of the binaries are already bundled in the package, except for the following:
// - Linux/x64/CUDA 11
// - Linux/x64/CUDA 12
//
// The CUDA binaries are not bundled because they are too large to be allowed in the npm registry. Instead, they are
// downloaded from the GitHub release page of ONNX Runtime. The script will download the binaries if they are not
// already present in the package.

// Step.1: Check if we should exit early
const os = require('os');
const fs = require('fs');
const https = require('https');
const path = require('path');
const tar = require('tar');
const { execFileSync } = require('child_process');
const { bootstrap: globalAgentBootstrap } = require('global-agent');

// Bootstrap global-agent to honor the proxy settings in
// environment variables, e.g. GLOBAL_AGENT_HTTPS_PROXY.
// See https://github.com/gajus/global-agent/blob/v3.0.0/README.md#environment-variables for details.
globalAgentBootstrap();

// commandline flag:
// --onnxruntime-node-install-cuda         Force install the CUDA EP binaries. Try to detect the CUDA version.
// --onnxruntime-node-install-cuda=v11     Force install the CUDA EP binaries for CUDA 11.
// --onnxruntime-node-install-cuda=v12     Force install the CUDA EP binaries for CUDA 12.
// --onnxruntime-node-install-cuda=skip    Skip the installation of the CUDA EP binaries.
//
// Alternatively, use environment variable "ONNXRUNTIME_NODE_INSTALL_CUDA"
//
// If the flag is not provided, the script will only install the CUDA EP binaries when:
// - The platform is Linux/x64.
// - The binaries are not already present in the package.
// - The installation is not a local install (when used inside ONNX Runtime repo).
//
const INSTALL_CUDA_FLAG = parseInstallCudaFlag();
const NO_INSTALL = INSTALL_CUDA_FLAG === 'skip';
const FORCE_INSTALL = !NO_INSTALL && INSTALL_CUDA_FLAG;

const IS_LINUX_X64 = os.platform() === 'linux' && os.arch() === 'x64';
const BIN_FOLDER = path.join(__dirname, '..', 'bin/napi-v3/linux/x64');
const BIN_FOLDER_EXISTS = fs.existsSync(BIN_FOLDER);
const CUDA_DLL_EXISTS = fs.existsSync(path.join(BIN_FOLDER, 'libonnxruntime_providers_cuda.so'));
const ORT_VERSION = require('../package.json').version;

const npm_config_local_prefix = process.env.npm_config_local_prefix;
const npm_package_json = process.env.npm_package_json;
const SKIP_LOCAL_INSTALL =
  npm_config_local_prefix && npm_package_json && path.dirname(npm_package_json) === npm_config_local_prefix;

const shouldInstall = FORCE_INSTALL || (!SKIP_LOCAL_INSTALL && IS_LINUX_X64 && BIN_FOLDER_EXISTS && !CUDA_DLL_EXISTS);
if (NO_INSTALL || !shouldInstall) {
  process.exit(0);
}

// Step.2: Download the required binaries
const artifactUrl = {
  get 11() {
    // TODO: support ORT Cuda v11 binaries
    throw new Error(`CUDA 11 binaries are not supported by this script yet.

To use ONNX Runtime Node.js binding with CUDA v11 support, please follow the manual steps:

1. Use "--onnxruntime-node-install-cuda=skip" to skip the auto installation.
2. Navigate to https://aiinfra.visualstudio.com/PublicPackages/_artifacts/feed/onnxruntime-cuda-11
3. Download the binaries for your platform and architecture
4. Extract the following binaries to "node_modules/onnxruntime-node/bin/napi-v3/linux/x64:
   - libonnxruntime_providers_tensorrt.so
   - libonnxruntime_providers_shared.so
   - libonnxruntime.so.${ORT_VERSION}
   - libonnxruntime_providers_cuda.so
`);
  },
  12: `https://github.com/microsoft/onnxruntime/releases/download/v${ORT_VERSION}/onnxruntime-linux-x64-gpu-${
    ORT_VERSION
  }.tgz`,
}[INSTALL_CUDA_FLAG || tryGetCudaVersion()];
console.log(`Downloading "${artifactUrl}"...`);

const FILES = new Set([
  'libonnxruntime_providers_tensorrt.so',
  'libonnxruntime_providers_shared.so',
  `libonnxruntime.so.${ORT_VERSION}`,
  'libonnxruntime_providers_cuda.so',
]);

downloadAndExtract(artifactUrl, BIN_FOLDER, FILES);

async function downloadAndExtract(url, dest, files) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      const { statusCode } = res;
      const contentType = res.headers['content-type'];

      if (statusCode === 301 || statusCode === 302) {
        downloadAndExtract(res.headers.location, dest, files).then(
          (value) => resolve(value),
          (reason) => reject(reason),
        );
        return;
      } else if (statusCode !== 200) {
        throw new Error(`Failed to download the binaries: ${res.statusCode} ${res.statusMessage}.

Use "--onnxruntime-node-install-cuda=skip" to skip the installation. You will still be able to use ONNX Runtime, but the CUDA EP will not be available.`);
      }

      if (!contentType || !/^application\/octet-stream/.test(contentType)) {
        throw new Error(`unexpected content type: ${contentType}`);
      }

      res
        .pipe(
          tar.t({
            strict: true,
            onentry: (entry) => {
              const filename = path.basename(entry.path);
              if (entry.type === 'File' && files.has(filename)) {
                console.log(`Extracting "${filename}" to "${dest}"...`);
                entry.pipe(fs.createWriteStream(path.join(dest, filename)));
                entry.on('finish', () => {
                  console.log(`Finished extracting "${filename}".`);
                });
              }
            },
          }),
        )
        .on('error', (err) => {
          throw new Error(`Failed to extract the binaries: ${err.message}.

Use "--onnxruntime-node-install-cuda=skip" to skip the installation. You will still be able to use ONNX Runtime, but the CUDA EP will not be available.`);
        });
    });
  });
}

function tryGetCudaVersion() {
  // Should only return 11 or 12.

  // try to get the CUDA version from the system ( `nvcc --version` )
  let ver = 12;
  try {
    const nvccVersion = execFileSync('nvcc', ['--version'], { encoding: 'utf8' });
    const match = nvccVersion.match(/release (\d+)/);
    if (match) {
      ver = parseInt(match[1]);
      if (ver !== 11 && ver !== 12) {
        throw new Error(`Unsupported CUDA version: ${ver}`);
      }
    }
  } catch (e) {
    if (e?.code === 'ENOENT') {
      console.warn('`nvcc` not found. Assuming CUDA 12.');
    } else {
      console.warn('Failed to detect CUDA version from `nvcc --version`:', e.message);
    }
  }

  // assume CUDA 12 if failed to detect
  return ver;
}

function parseInstallCudaFlag() {
  let flag = process.env.ONNXRUNTIME_NODE_INSTALL_CUDA || process.env.npm_config_onnxruntime_node_install_cuda;
  if (!flag) {
    for (let i = 0; i < process.argv.length; i++) {
      if (process.argv[i].startsWith('--onnxruntime-node-install-cuda=')) {
        flag = process.argv[i].split('=')[1];
        break;
      } else if (process.argv[i] === '--onnxruntime-node-install-cuda') {
        flag = 'true';
      }
    }
  }
  switch (flag) {
    case 'true':
    case '1':
    case 'ON':
      return tryGetCudaVersion();
    case 'v11':
      return 11;
    case 'v12':
      return 12;
    case 'skip':
    case undefined:
      return flag;
    default:
      throw new Error(`Invalid value for --onnxruntime-node-install-cuda: ${flag}`);
  }
}
