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
Object.defineProperty(exports, "__esModule", { value: true });
const fs = __importStar(require("fs-extra"));
const path = __importStar(require("path"));
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
