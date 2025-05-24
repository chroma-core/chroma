"use strict";
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
exports.FixedPortGenerator = exports.RandomUniquePortGenerator = void 0;
class RandomPortGenerator {
    async generatePort() {
        const { default: getPort } = await Promise.resolve().then(() => __importStar(require("get-port")));
        return getPort();
    }
}
class RandomUniquePortGenerator {
    portGenerator;
    static assignedPorts = new Set();
    constructor(portGenerator = new RandomPortGenerator()) {
        this.portGenerator = portGenerator;
    }
    async generatePort() {
        let port;
        do {
            port = await this.portGenerator.generatePort();
        } while (RandomUniquePortGenerator.assignedPorts.has(port));
        RandomUniquePortGenerator.assignedPorts.add(port);
        return port;
    }
}
exports.RandomUniquePortGenerator = RandomUniquePortGenerator;
class FixedPortGenerator {
    ports;
    portIndex = 0;
    constructor(ports) {
        this.ports = ports;
    }
    generatePort() {
        return Promise.resolve(this.ports[this.portIndex++]);
    }
}
exports.FixedPortGenerator = FixedPortGenerator;
//# sourceMappingURL=port-generator.js.map