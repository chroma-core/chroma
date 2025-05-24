'use strict';
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
Object.defineProperty(exports, '__esModule', { value: true });
exports.reshape = void 0;
const util_1 = require('../../../util');
const reshape = (handler, inputs) => {
  const reshapedDims = util_1.ShapeUtil.calculateReshapedDims(inputs[0].dims, inputs[1].integerData);
  if (handler.session.pack) {
    return [handler.reshapePacked(inputs[0], reshapedDims)];
  } else {
    return [handler.reshapeUnpacked(inputs[0], reshapedDims)];
  }
};
exports.reshape = reshape;
//# sourceMappingURL=reshape.js.map
