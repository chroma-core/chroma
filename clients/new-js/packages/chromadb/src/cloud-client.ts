import { ChromaClient } from "./chroma-client";
import * as process from "node:process";
import { AdminClient } from "./admin-client";
import { ChromaUnauthorizedError, ChromaValueError } from "./errors";

/**
 * ChromaDB cloud client for connecting to hosted Chroma instances.
 * Extends ChromaClient with cloud-specific authentication and configuration.
 */
export class CloudClient extends ChromaClient {
  /**
   * Creates a new CloudClient instance for Chroma Cloud.
   * @param args - Cloud client configuration options
   */
  constructor(
    args: Partial<{
      /** API key for authentication (or set CHROMA_API_KEY env var) */
      apiKey?: string;
      /** Tenant name for multi-tenant deployments */
      tenant?: string;
      /** Database name to connect to */
      database?: string;
      /** Additional fetch options for HTTP requests */
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

  /** @ignore */
  override async _path(): Promise<{ tenant: string; database: string }> {
    if (!this.tenant || !this.database) {
      const { tenant, databases } = await this.getUserIdentity();
      this.tenant = tenant;
      if (databases.length === 0) {
        throw new ChromaUnauthorizedError(
          `Your API key does not have access to any DBs for tenant ${this.tenant}`,
        );
      }
      if (databases.length > 1 || databases[0] === "*") {
        throw new ChromaValueError(
          "Your API key is scoped to more than 1 DB. Please provide a DB name to the CloudClient constructor",
        );
      }
      this.database = databases[0];
    }
    return super._path();
  }
}

/**
 * Admin client for Chroma Cloud administrative operations.
 * Extends AdminClient with cloud-specific authentication.
 */
export class AdminCloudClient extends AdminClient {
  /**
   * Creates a new AdminCloudClient instance for cloud admin operations.
   * @param args - Admin cloud client configuration options
   */
  constructor(
    args: Partial<{
      /** API key for authentication (or set CHROMA_API_KEY env var) */
      apiKey?: string;
      /** Additional fetch options for HTTP requests */
      fetchOptions?: RequestInit;
    }> = {},
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
