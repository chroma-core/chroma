import { DefaultApi } from "./generated/api";
import { Configuration } from "./generated/configuration";

const basePath: string = "http://localhost:8000";

const apiConfig: Configuration = new Configuration({
  basePath,
});

export const chromaClient: DefaultApi = new DefaultApi(apiConfig);

