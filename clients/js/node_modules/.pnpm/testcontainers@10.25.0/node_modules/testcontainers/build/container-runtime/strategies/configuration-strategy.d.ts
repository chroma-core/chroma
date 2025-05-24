import { ContainerRuntimeClientStrategy } from "./strategy";
import { ContainerRuntimeClientStrategyResult } from "./types";
export declare class ConfigurationStrategy implements ContainerRuntimeClientStrategy {
    private dockerHost;
    private dockerTlsVerify;
    private dockerCertPath;
    getName(): string;
    getResult(): Promise<ContainerRuntimeClientStrategyResult | undefined>;
}
