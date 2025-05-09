import { DEFAULT_TENANT, HttpMethod, normalizeMethod } from "./utils";
import { createClient, createConfig } from "@hey-api/client-fetch";
import { DefaultService as Api } from "./api";

export interface AdminClientArgs {
  host: string;
  port: number;
  ssl: boolean;
  tenant: string;
  headers?: Record<string, string>;
  fetchOptions?: RequestInit;
}

export class AdminClient {
  private readonly tenant: string;
  private readonly apiClient: ReturnType<typeof createClient>;
  private readonly headers?: Record<string, string>;

  constructor({
    host = "localhost",
    port = 8000,
    ssl = false,
    tenant = DEFAULT_TENANT,
    headers = undefined,
    fetchOptions = undefined,
  }: AdminClientArgs) {
    const baseUrl = `${ssl ? "https" : "http"}://${host}:${port}`;

    this.tenant = tenant;
    this.headers = headers;

    const configOptions = {
      ...fetchOptions,
      method: normalizeMethod(fetchOptions?.method) as HttpMethod,
      baseUrl,
      headers,
    };

    this.apiClient = createClient(createConfig(configOptions));
  }

  public async createDatabase({ name }: { name: string }) {
    const { data, error } = await Api.createDatabase({
      client: this.apiClient,
      path: { tenant: this.tenant },
      body: { name },
    });
  }
}
