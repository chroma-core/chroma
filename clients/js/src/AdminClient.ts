import { Configuration, ApiApi as DefaultApi } from "./generated";
import { validateTenantDatabase } from "./utils";
import { ConfigOptions } from "./types";
import {
  AuthOptions,
  authOptionsToAuthProvider,
  ClientAuthProvider,
} from "./auth";
import { chromaFetch } from "./ChromaFetch";

const DEFAULT_TENANT = "default_tenant";
const DEFAULT_DATABASE = "default_database";

// interface for tenant
interface Tenant {
  name: string;
}

// interface for tenant
interface Database {
  name: string;
}

export class AdminClient {
  /**
   * @ignore
   */
  private api: DefaultApi & ConfigOptions;
  private authProvider: ClientAuthProvider | undefined;
  public tenant: string = DEFAULT_TENANT;
  public database: string = DEFAULT_DATABASE;

  /**
   * Creates a new AdminClient instance.
   * @param {Object} params - The parameters for creating a new client
   * @param {string} [params.path] - The base path for the Chroma API.
   * @returns {AdminClient} A new AdminClient instance.
   *
   * @example
   * ```typescript
   * const client = new AdminClient({
   *   path: "http://localhost:8000"
   * });
   * ```
   */
  constructor({
    path,
    fetchOptions,
    auth,
    tenant = DEFAULT_TENANT,
    database = DEFAULT_DATABASE,
  }: {
    path?: string;
    fetchOptions?: RequestInit;
    auth?: AuthOptions;
    tenant?: string;
    database?: string;
  } = {}) {
    if (path === undefined) path = "http://localhost:8000";
    this.tenant = tenant;
    this.database = database;
    this.authProvider = undefined;

    const apiConfig: Configuration = new Configuration({
      basePath: path,
    });

    this.api = new DefaultApi(apiConfig, undefined, chromaFetch);
    this.api.options = fetchOptions ?? {};

    if (auth !== undefined) {
      this.authProvider = authOptionsToAuthProvider(auth);
      this.api.options.headers = {
        ...this.api.options.headers,
        ...this.authProvider.authenticate(),
      };
    }
  }

  /**
   * Sets the tenant and database for the client.
   *
   * @param {Object} params - The parameters for setting tenant and database.
   * @param {string} params.tenant - The name of the tenant.
   * @param {string} params.database - The name of the database.
   *
   * @returns {Promise<void>} A promise that returns nothing
   * @throws {Error} Any issues
   *
   * @example
   * ```typescript
   * await adminClient.setTenant({
   *   tenant: "my_tenant",
   *   database: "my_database",
   * });
   * ```
   */
  public async setTenant({
    tenant = DEFAULT_TENANT,
    database = DEFAULT_DATABASE,
  }: {
    tenant: string;
    database?: string;
  }): Promise<void> {
    await validateTenantDatabase(this, tenant, database);
    this.tenant = tenant;
    this.database = database;
  }

  /**
   * Sets the database for the client.
   *
   * @param {Object} params - The parameters for setting the database.
   * @param {string} params.database - The name of the database.
   *
   * @returns {Promise<void>} A promise that returns nothing
   * @throws {Error} Any issues
   *
   * @example
   * ```typescript
   * await adminClient.setDatabase({
   *   database: "my_database",
   * });
   * ```
   */
  public async setDatabase({
    database = DEFAULT_DATABASE,
  }: {
    database?: string;
  }): Promise<void> {
    await validateTenantDatabase(this, this.tenant, database);
    this.database = database;
  }

  /**
   * Creates a new tenant with the specified properties.
   *
   * @param {Object} params - The parameters for creating a new tenant.
   * @param {string} params.name - The name of the tenant.
   *
   * @returns {Promise<Tenant>} A promise that resolves to the created tenant.
   * @throws {Error} If there is an issue creating the tenant.
   *
   * @example
   * ```typescript
   * await adminClient.createTenant({
   *   name: "my_tenant",
   * });
   * ```
   */
  public async createTenant({ name }: { name: string }): Promise<Tenant> {
    await this.api.createTenant({ name }, this.api.options);

    return { name };
  }

  /**
   * Gets a tenant with the specified properties.
   *
   * @param {Object} params - The parameters for getting a tenant.
   * @param {string} params.name - The name of the tenant.
   *
   * @returns {Promise<Tenant>} A promise that resolves to the tenant.
   * @throws {Error} If there is an issue getting the tenant.
   *
   * @example
   * ```typescript
   * await adminClient.getTenant({
   *   name: "my_tenant",
   * });
   * ```
   */
  public async getTenant({ name }: { name: string }): Promise<Tenant> {
    const getTenant = (await this.api.getTenant(
      name,
      this.api.options,
    )) as Tenant;

    return { name: getTenant.name };
  }

  /**
   * Creates a new database with the specified properties.
   *
   * @param {Object} params - The parameters for creating a new database.
   * @param {string} params.name - The name of the database.
   * @param {string} params.tenantName - The name of the tenant.
   *
   * @returns {Promise<Database>} A promise that resolves to the created database.
   * @throws {Error} If there is an issue creating the database.
   *
   * @example
   * ```typescript
   * await adminClient.createDatabase({
   *   name: "my_database",
   *   tenantName: "my_tenant",
   * });
   * ```
   */
  public async createDatabase({
    name,
    tenantName,
  }: {
    name: string;
    tenantName: string;
  }): Promise<Database> {
    await this.api.createDatabase(tenantName, { name }, this.api.options);

    return { name };
  }

  /**
   * Gets a database with the specified properties.
   *
   * @param {Object} params - The parameters for getting a database.
   * @param {string} params.name - The name of the database.
   * @param {string} params.tenantName - The name of the tenant.
   *
   * @returns {Promise<Database>} A promise that resolves to the database.
   * @throws {Error} If there is an issue getting the database.
   *
   * @example
   * ```typescript
   * await adminClient.getDatabase({
   *   name: "my_database",
   *   tenantName: "my_tenant",
   * });
   * ```
   */
  public async getDatabase({
    name,
    tenantName,
  }: {
    name: string;
    tenantName: string;
  }): Promise<Database> {
    const getDatabase = (await this.api.getDatabase(
      name,
      tenantName,
      this.api.options,
    )) as Database;

    return { name: getDatabase.name } as Database;
  }
}
