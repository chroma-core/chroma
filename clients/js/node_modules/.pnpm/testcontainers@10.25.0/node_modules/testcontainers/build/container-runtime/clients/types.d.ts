export type Info = {
    node: NodeInfo;
    containerRuntime: ContainerRuntimeInfo;
    compose: ComposeInfo;
};
export type NodeInfo = {
    version: string;
    architecture: string;
    platform: string;
};
export type ContainerRuntimeInfo = {
    host: string;
    hostIps: HostIp[];
    remoteSocketPath: string;
    indexServerAddress: string;
    serverVersion: number;
    operatingSystem: string;
    operatingSystemType: string;
    architecture: string;
    cpus: number;
    memory: number;
    runtimes: string[];
    labels: string[];
};
export type ComposeInfo = {
    version: string;
    compatability: "v1" | "v2";
} | undefined;
export type HostIp = {
    address: string;
    family: number;
};
