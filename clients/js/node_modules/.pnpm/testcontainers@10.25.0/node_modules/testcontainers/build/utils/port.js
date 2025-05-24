"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.hasHostBinding = exports.getContainerPort = void 0;
const getContainerPort = (port) => typeof port === "number" ? port : port.container;
exports.getContainerPort = getContainerPort;
const hasHostBinding = (port) => {
    return typeof port === "object" && port.host !== undefined;
};
exports.hasHostBinding = hasHostBinding;
//# sourceMappingURL=port.js.map