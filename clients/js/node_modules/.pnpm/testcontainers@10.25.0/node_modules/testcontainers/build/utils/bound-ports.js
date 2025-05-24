"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.resolveHostPortBinding = exports.BoundPorts = void 0;
const net_1 = __importDefault(require("net"));
const port_1 = require("./port");
class BoundPorts {
    ports = new Map();
    getBinding(port) {
        const binding = this.ports.get(port);
        if (!binding) {
            throw new Error(`No port binding found for :${port}`);
        }
        return binding;
    }
    getFirstBinding() {
        const firstBinding = this.ports.values().next().value;
        if (!firstBinding) {
            throw new Error("No port bindings found");
        }
        else {
            return firstBinding;
        }
    }
    setBinding(key, value) {
        this.ports.set(key, value);
    }
    iterator() {
        return this.ports;
    }
    filter(ports) {
        const boundPorts = new BoundPorts();
        const containerPorts = ports.map((port) => (0, port_1.getContainerPort)(port));
        for (const [internalPort, hostPort] of this.iterator()) {
            if (containerPorts.includes(internalPort)) {
                boundPorts.setBinding(internalPort, hostPort);
            }
        }
        return boundPorts;
    }
    static fromInspectResult(hostIps, inspectResult) {
        const boundPorts = new BoundPorts();
        Object.entries(inspectResult.ports).forEach(([containerPort, hostBindings]) => {
            const hostPort = (0, exports.resolveHostPortBinding)(hostIps, hostBindings);
            boundPorts.setBinding(parseInt(containerPort), hostPort);
        });
        return boundPorts;
    }
}
exports.BoundPorts = BoundPorts;
const resolveHostPortBinding = (hostIps, hostPortBindings) => {
    if (isDualStackIp(hostPortBindings)) {
        return hostPortBindings[0].hostPort;
    }
    for (const { family } of hostIps) {
        const hostPortBinding = hostPortBindings.find(({ hostIp }) => net_1.default.isIP(hostIp) === family);
        if (hostPortBinding !== undefined) {
            return hostPortBinding.hostPort;
        }
    }
    throw new Error("No host port found for host IP");
};
exports.resolveHostPortBinding = resolveHostPortBinding;
const isDualStackIp = (hostPortBindings) => hostPortBindings.length === 1 && hostPortBindings[0].hostIp === "";
//# sourceMappingURL=bound-ports.js.map