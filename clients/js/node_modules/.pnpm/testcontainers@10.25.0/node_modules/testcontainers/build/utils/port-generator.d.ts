export interface PortGenerator {
    generatePort(): Promise<number>;
}
export declare class RandomUniquePortGenerator implements PortGenerator {
    private readonly portGenerator;
    private static readonly assignedPorts;
    constructor(portGenerator?: PortGenerator);
    generatePort(): Promise<number>;
}
export declare class FixedPortGenerator implements PortGenerator {
    private readonly ports;
    private portIndex;
    constructor(ports: number[]);
    generatePort(): Promise<number>;
}
