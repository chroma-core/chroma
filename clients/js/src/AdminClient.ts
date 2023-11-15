import { Configuration, ApiApi as DefaultApi } from "./generated";
import { handleSuccess, handleError } from "./utils";
import { ConfigOptions } from './types';
import {
    AuthOptions,
    ClientAuthProtocolAdapter,
    IsomorphicFetchClientAuthProtocolAdapter
} from "./auth";

const DEFAULT_TENANT = "default_tenant"
const DEFAULT_DATABASE = "default_database"

// interface for tenant
interface Tenant {
    name: string,
}

// interface for tenant
interface Database {
    name: string,
}

export class AdminClient {
    /**
     * @ignore
     */
    private api: DefaultApi & ConfigOptions;
    private apiAdapter: ClientAuthProtocolAdapter<any>|undefined;
    private tenant: string = DEFAULT_TENANT;
    private database: string = DEFAULT_DATABASE;

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
        database = DEFAULT_DATABASE
    }: {
        path?: string,
        fetchOptions?: RequestInit,
        auth?: AuthOptions,
        tenant?: string,
        database?: string,
    } = {}) {
        if (path === undefined) path = "http://localhost:8000";
        this.tenant = tenant;
        this.database = database;

        const apiConfig: Configuration = new Configuration({
            basePath: path,
        });
        if (auth !== undefined) {
            this.apiAdapter = new IsomorphicFetchClientAuthProtocolAdapter(new DefaultApi(apiConfig), auth);
            this.api = this.apiAdapter.getApi();
        } else {
            this.api = new DefaultApi(apiConfig);
        }

        this.api.options = fetchOptions ?? {};
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
    public async createTenant({
        name,
    }: {
        name: string,
    }): Promise<Tenant> {
        const newTenant = await this.api
            .createTenant({name}, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        // TODO: newTenant is null...... is this a bug in the backend API?
        // if (newTenant.error) {
        //     throw new Error(newTenant.error);
        // }

        return {name: name} as Tenant
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
    public async getTenant({
        name,
    }: {
        name: string,
    }): Promise<Tenant> {
        const getTenant = await this.api
            .getTenant(name, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        if (getTenant.error) {
            throw new Error(getTenant.error);
        }

        return {name: getTenant.name} as Tenant
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
        tenantName
    }: {
        name: string,
        tenantName: string,
    }): Promise<Database> {
        const newDatabase = await this.api
            .createDatabase(tenantName, {name}, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        // TODO: newDatabase is null...... is this a bug in the backend API?
        // if (newDatabase.error) {
        //     throw new Error(newDatabase.error);
        // }

        return {name: name} as Database
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
        tenantName
    }: {
        name: string,
        tenantName: string,
    }): Promise<Database> {
        const getDatabase = await this.api
            .getDatabase(name, tenantName, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        if (getDatabase.error) {
            throw new Error(getDatabase.error);
        }

        return {name: getDatabase.name} as Database
    }

}
