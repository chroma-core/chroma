import Dockerode from "dockerode";
import { Uuid } from "../common";
import { ContainerRuntimeClient } from "../container-runtime";
export declare class Network {
    private readonly uuid;
    constructor(uuid?: Uuid);
    start(): Promise<StartedNetwork>;
}
export declare class StartedNetwork {
    private readonly client;
    private readonly name;
    private readonly network;
    constructor(client: ContainerRuntimeClient, name: string, network: Dockerode.Network);
    getId(): string;
    getName(): string;
    stop(): Promise<StoppedNetwork>;
}
export declare class StoppedNetwork {
}
