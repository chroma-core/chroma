import { ChromaClient } from "./chroma-client";
import * as process from "node:process";
import { AdminClient } from "./admin-client";
import { ChromaValueError } from "./errors";

export class CloudClient extends ChromaClient {
  constructor(
    args: Partial<{
      apiKey?: string;
      tenant?: string;
      database?: string;
      fetchOptions?: RequestInit;
    }> = {},
  ) {
    const apiKey = args.apiKey || process.env.CHROMA_API_KEY;
    if (!apiKey) {
      throw new ChromaValueError(
        "Missing API key. Please provide it to the CloudClient constructor or set your CHROMA_API_KEY environment variable",
      );
    }

    const tenant = args.tenant || process.env.CHROMA_TENANT;
    const database = args.database || process.env.CHROMA_DATABASE;

    super({
      host: "api.trychroma.com",
      port: 8000,
      ssl: true,
      tenant,
      database,
      headers: { "x-chroma-token": apiKey },
      fetchOptions: args.fetchOptions,
    });

    // Override from ChromaClient construction in case undefined. This will trigger auto-resolution in the "path" function
    this.tenant = tenant;
    this.database = database;
  }
}

export class AdminCloudClient extends AdminClient {
  constructor(
    args: Partial<{ apiKey?: string; fetchOptions?: RequestInit }> = {},
  ) {
    const apiKey = args.apiKey || process.env.CHROMA_API_KEY;
    if (!apiKey) {
      throw new ChromaValueError(
        "Missing API key. Please provide it to the CloudClient constructor or set your CHROMA_API_KEY environment variable",
      );
    }

    super({
      host: "api.trychroma.com",
      port: 8000,
      ssl: true,
      headers: { "x-chroma-token": apiKey },
      fetchOptions: args.fetchOptions,
    });
  }
}
