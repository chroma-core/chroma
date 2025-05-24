// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import * as fs from 'fs-extra';
import * as path from 'path';

function updatePackageJson() {
  const commonPackageJsonPath = path.join(__dirname, '..', '..', 'common', 'package.json');
  const selfPackageJsonPath = path.join(__dirname, '..', 'package.json');
  console.log(`=== start to update package.json: ${selfPackageJsonPath}`);
  const packageCommon = fs.readJSONSync(commonPackageJsonPath);
  const packageSelf = fs.readJSONSync(selfPackageJsonPath);
  const version = packageCommon.version;
  packageSelf.dependencies['onnxruntime-common'] = `${version}`;
  fs.writeJSONSync(selfPackageJsonPath, packageSelf, { spaces: 2 });
  console.log('=== finished updating package.json.');
}

// update version of dependency "onnxruntime-common" before packing
updatePackageJson();
