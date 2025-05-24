import { HostIp } from "../container-runtime";
import { HostPortBindings, InspectResult } from "../types";
import { PortWithOptionalBinding } from "./port";
export declare class BoundPorts {
    private readonly ports;
    getBinding(port: number): number;
    getFirstBinding(): number;
    setBinding(key: number, value: number): void;
    iterator(): Iterable<[number, number]>;
    filter(ports: PortWithOptionalBinding[]): BoundPorts;
    static fromInspectResult(hostIps: HostIp[], inspectResult: InspectResult): BoundPorts;
}
export declare const resolveHostPortBinding: (hostIps: HostIp[], hostPortBindings: HostPortBindings) => number;
