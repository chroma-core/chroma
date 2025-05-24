/// <reference types="node" />
import { Logger } from "../../../common";
export type ComposeOptions = {
    filePath: string;
    files: string | string[];
    projectName: string;
    commandOptions?: string[];
    composeOptions?: string[];
    environment?: NodeJS.ProcessEnv;
    logger?: Logger;
};
export type ComposeDownOptions = {
    timeout: number;
    removeVolumes: boolean;
};
