// create a cloudclient class that takes in an api key and an optional database
// this should wrap ChromaClient and specify the auth scheme correctly

import { ChromaClient } from "./ChromaClient";
import { AuthOptions } from "./auth";

interface CloudClientParams {
  apiKey?: string;
  database?: string;
  tenant?: string;
  cloudHost?: string;
  cloudPort?: string;
}

class CloudClient extends ChromaClient {
  constructor({
    apiKey,
    database,
    tenant,
    cloudHost,
    cloudPort,
  }: CloudClientParams) {
    // If no API key is provided, try to load it from the environment variable
    if (!apiKey) {
      apiKey = process.env.CHROMA_API_KEY;
    }
    if (!apiKey) {
      throw new Error("No API key provided");
    }

    cloudHost = cloudHost || "https://api.trychroma.com";
    cloudPort = cloudPort || "8000";

    const path = `${cloudHost}:${cloudPort}`;

    const auth: AuthOptions = {
      provider: "token",
      credentials: apiKey,
      tokenHeaderType: "X_CHROMA_TOKEN",
    };

    return new ChromaClient({
      path: path,
      auth: auth,
      database,
      tenant,
    });

    super();
  }
}

export { CloudClient };
