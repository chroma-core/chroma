import { IEmbeddingFunction } from './embeddings/IEmbeddingFunction';
import { Configuration, ApiApi as DefaultApi } from "./generated";
import { handleSuccess, handleError } from "./utils";
import { Collection } from './Collection';
import { CollectionMetadata, CollectionType, ConfigOptions } from './types';
import {
    AuthOptions,
    ClientAuthProtocolAdapter,
    IsomorphicFetchClientAuthProtocolAdapter
} from "./auth";


export class ChromaClient {
    /**
     * @ignore
     */
    private api: DefaultApi & ConfigOptions;
    private apiAdapter: ClientAuthProtocolAdapter<any>|undefined;

    /**
     * Creates a new ChromaClient instance.
     * @param {Object} params - The parameters for creating a new client
     * @param {string} [params.path] - The base path for the Chroma API.
     * @returns {ChromaClient} A new ChromaClient instance.
     *
     * @example
     * ```typescript
     * const client = new ChromaClient({
     *   path: "http://localhost:8000"
     * });
     * ```
     */
    constructor({
        path,
        fetchOptions,
        auth,
    }: {
        path?: string,
        fetchOptions?: RequestInit,
        auth?: AuthOptions,
    } = {}) {
        if (path === undefined) path = "http://localhost:8000";
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
     * Resets the state of the object by making an API call to the reset endpoint.
     *
     * @returns {Promise<boolean>} A promise that resolves when the reset operation is complete.
     * @throws {Error} If there is an issue resetting the state.
     *
     * @example
     * ```typescript
     * await client.reset();
     * ```
     */
    public async reset(): Promise<boolean> {
        return await this.api.reset(this.api.options);
    }

    /**
     * Returns the version of the Chroma API.
     * @returns {Promise<string>} A promise that resolves to the version of the Chroma API.
     *
     * @example
     * ```typescript
     * const version = await client.version();
     * ```
     */
    public async version(): Promise<string> {
        const response = await this.api.version(this.api.options);
        return await handleSuccess(response);
    }

    /**
     * Returns a heartbeat from the Chroma API.
     * @returns {Promise<number>} A promise that resolves to the heartbeat from the Chroma API.
     *
     * @example
     * ```typescript
     * const heartbeat = await client.heartbeat();
     * ```
     */
    public async heartbeat(): Promise<number> {
        const response = await this.api.heartbeat(this.api.options);
        let ret = await handleSuccess(response);
        return ret["nanosecond heartbeat"]
    }

    /**
     * Creates a new collection with the specified properties.
     *
     * @param {Object} params - The parameters for creating a new collection.
     * @param {string} params.name - The name of the collection.
     * @param {CollectionMetadata} [params.metadata] - Optional metadata associated with the collection.
     * @param {IEmbeddingFunction} [params.embeddingFunction] - Optional custom embedding function for the collection.
     *
     * @returns {Promise<Collection>} A promise that resolves to the created collection.
     * @throws {Error} If there is an issue creating the collection.
     *
     * @example
     * ```typescript
     * const collection = await client.createCollection({
     *   name: "my_collection",
     *   metadata: {
     *     "description": "My first collection"
     *   }
     * });
     * ```
     */
    public async createCollection({
        name,
        metadata,
        embeddingFunction
    }: {
        name: string,
        metadata?: CollectionMetadata,
        embeddingFunction?: IEmbeddingFunction
    }): Promise<Collection> {
        const newCollection = await this.api
            .createCollection({
                name,
                metadata,
            }, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        if (newCollection.error) {
            throw new Error(newCollection.error);
        }

        return new Collection(name, newCollection.id, this.api, metadata, embeddingFunction);
    }

    /**
     * Gets or creates a collection with the specified properties.
     *
     * @param {Object} params - The parameters for creating a new collection.
     * @param {string} params.name - The name of the collection.
     * @param {CollectionMetadata} [params.metadata] - Optional metadata associated with the collection.
     * @param {IEmbeddingFunction} [params.embeddingFunction] - Optional custom embedding function for the collection.
     *
     * @returns {Promise<Collection>} A promise that resolves to the got or created collection.
     * @throws {Error} If there is an issue getting or creating the collection.
     *
     * @example
     * ```typescript
     * const collection = await client.getOrCreateCollection({
     *   name: "my_collection",
     *   metadata: {
     *     "description": "My first collection"
     *   }
     * });
     * ```
     */
    public async getOrCreateCollection({
        name,
        metadata,
        embeddingFunction
    }: {
        name: string,
        metadata?: CollectionMetadata,
        embeddingFunction?: IEmbeddingFunction
    }): Promise<Collection> {
        const newCollection = await this.api
            .createCollection({
                name,
                metadata,
                'get_or_create': true
            }, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        if (newCollection.error) {
            throw new Error(newCollection.error);
        }

        return new Collection(
            name,
            newCollection.id,
            this.api,
            newCollection.metadata,
            embeddingFunction
        );
    }

    /**
     * Lists all collections.
     *
     * @returns {Promise<CollectionType[]>} A promise that resolves to a list of collection names.
     * @throws {Error} If there is an issue listing the collections.
     *
     * @example
     * ```typescript
     * const collections = await client.listCollections();
     * ```
     */
    public async listCollections(): Promise<CollectionType[]> {
        const response = await this.api.listCollections(this.api.options);
        return handleSuccess(response);
    }

    /**
     * Gets a collection with the specified name.
     * @param {Object} params - The parameters for getting a collection.
     * @param {string} params.name - The name of the collection.
     * @param {IEmbeddingFunction} [params.embeddingFunction] - Optional custom embedding function for the collection.
     * @returns {Promise<Collection>} A promise that resolves to the collection.
     * @throws {Error} If there is an issue getting the collection.
     *
     * @example
     * ```typescript
     * const collection = await client.getCollection({
     *   name: "my_collection"
     * });
     * ```
     */
    public async getCollection({
        name,
        embeddingFunction
    }: {
        name: string;
        embeddingFunction?: IEmbeddingFunction
    }): Promise<Collection> {
        const response = await this.api
            .getCollection(name, this.api.options)
            .then(handleSuccess)
            .catch(handleError);

        if (response.error) {
            throw new Error(response.error);
        }

        return new Collection(
            response.name,
            response.id,
            this.api,
            response.metadata,
            embeddingFunction
        );

    }

    /**
     * Deletes a collection with the specified name.
     * @param {Object} params - The parameters for deleting a collection.
     * @param {string} params.name - The name of the collection.
     * @returns {Promise<void>} A promise that resolves when the collection is deleted.
     * @throws {Error} If there is an issue deleting the collection.
     *
     * @example
     * ```typescript
     * await client.deleteCollection({
     *  name: "my_collection"
     * });
     * ```
     */
    public async deleteCollection({
        name
    }: {
        name: string
    }): Promise<void> {
        return await this.api
            .deleteCollection(name, this.api.options)
            .then(handleSuccess)
            .catch(handleError);
    }

}
