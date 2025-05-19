import { ChromaClient } from "./chroma-client";
import * as process from "node:process";

export class CloudClient extends ChromaClient {
  private readonly apiKey: string;

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
      throw new Error(
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
    });

    this.apiKey = apiKey;

    this.tenant = tenant;
    this.database = database;
  }
}
