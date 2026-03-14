import path from "node:path";
import {
  startContainer as startContainerCommon,
  startChromaServer as startChromaServerCommon,
} from "./start-chroma-common";

// Use path.resolve to get to the project root for Jest/CommonJS environment
const BUILD_CONTEXT_DIR = path.resolve(__dirname, "../../../../..");

export const startContainer = async (verbose?: boolean) => {
  return startContainerCommon(BUILD_CONTEXT_DIR, verbose);
};

export const startChromaServer = async () => {
  return startChromaServerCommon(BUILD_CONTEXT_DIR);
};
