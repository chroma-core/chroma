import { fileURLToPath } from "url";
import { dirname } from "path";
import path from "node:path";
import {
  startContainer as startContainerCommon,
  startChromaServer as startChromaServerCommon,
} from "./start-chroma-common.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const BUILD_CONTEXT_DIR = path.join(__dirname, "../../../../..");

export const startContainer = async (verbose?: boolean) => {
  return startContainerCommon(BUILD_CONTEXT_DIR, verbose);
};

export const startChromaServer = async () => {
  return startChromaServerCommon(BUILD_CONTEXT_DIR);
};
