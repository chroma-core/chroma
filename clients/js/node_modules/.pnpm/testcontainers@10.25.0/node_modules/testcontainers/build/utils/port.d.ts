export type PortWithBinding = {
    container: number;
    host: number;
};
export type PortWithOptionalBinding = number | PortWithBinding;
export declare const getContainerPort: (port: PortWithOptionalBinding) => number;
export declare const hasHostBinding: (port: PortWithOptionalBinding) => port is PortWithBinding;
