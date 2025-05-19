import { defaultAdminClientArgs, HttpMethod, normalizeMethod } from "./utils";
import { createClient, createConfig } from "@hey-api/client-fetch";
import { Database, DefaultService as Api } from "./api";
import { chromaFetch } from "./chroma-fetch";

export interface AdminClientArgs {
  host: string;
  port: number;
  ssl: boolean;
  headers?: Record<string, string>;
  fetchOptions?: RequestInit;
}

export interface ListDatabasesArgs {
  tenant: string;
  limit?: number;
  offset?: number;
}

export class AdminClient {
  private readonly apiClient: ReturnType<typeof createClient>;

  constructor(args?: AdminClientArgs) {
    const { host, port, ssl, headers, fetchOptions } =
      args || defaultAdminClientArgs;

    const baseUrl = `${ssl ? "https" : "http"}://${host}:${port}`;

    const configOptions = {
      ...fetchOptions,
      method: normalizeMethod(fetchOptions?.method) as HttpMethod,
      baseUrl,
      headers,
    };

    this.apiClient = createClient(createConfig(configOptions));
    this.apiClient.setConfig({ fetch: chromaFetch });
  }

  public async createDatabase({
    name,
    tenant,
  }: {
    name: string;
    tenant: string;
  }): Promise<void> {
    await Api.createDatabase({
      client: this.apiClient,
      path: { tenant },
      body: { name },
    });
  }

  public async getDatabase({
    name,
    tenant,
  }: {
    name: string;
    tenant: string;
  }): Promise<Database> {
    const { data } = await Api.getDatabase({
      client: this.apiClient,
      path: { tenant, database: name },
    });

    return data;
  }

  public async deleteDatabase({
    name,
    tenant,
  }: {
    name: string;
    tenant: string;
  }): Promise<void> {
    await Api.deleteDatabase({
      client: this.apiClient,
      path: { tenant, database: name },
    });
  }

  public async listDatabases(args: ListDatabasesArgs): Promise<Database[]> {
    const { limit = 100, offset = 0, tenant } = args;
    const { data } = await Api.listDatabases({
      client: this.apiClient,
      path: { tenant },
      query: { limit, offset },
    });

    return data;
  }

  public async createTenant({ name }: { name: string }): Promise<void> {
    await Api.createTenant({
      client: this.apiClient,
      body: { name },
    });
  }

  public async getTenant({ name }: { name: string }): Promise<string> {
    const { data } = await Api.getTenant({
      client: this.apiClient,
      path: { tenant_name: name },
    });

    return data.name;
  }
}
