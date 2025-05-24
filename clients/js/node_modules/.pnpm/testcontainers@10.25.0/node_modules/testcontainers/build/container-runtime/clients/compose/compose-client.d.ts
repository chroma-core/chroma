/// <reference types="node" />
import { ComposeInfo } from "../types";
import { ComposeDownOptions, ComposeOptions } from "./types";
export interface ComposeClient {
    info: ComposeInfo;
    up(options: ComposeOptions, services?: Array<string>): Promise<void>;
    pull(options: ComposeOptions, services?: Array<string>): Promise<void>;
    stop(options: ComposeOptions): Promise<void>;
    down(options: ComposeOptions, downOptions: ComposeDownOptions): Promise<void>;
}
export declare function getComposeClient(environment: NodeJS.ProcessEnv): Promise<ComposeClient>;
