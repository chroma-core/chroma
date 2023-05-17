import { Configuration, ApiApi as DefaultApi } from "./generated";
import { handleError, handleSuccess, } from './utils';
import { IEmbeddingFunction } from './embeddings/IEmbeddingFunction';
import { Collection } from "./Collection";

export class ChromaClient {
    private api: DefaultApi;

    constructor(basePath?: string) {
        if (basePath === undefined) basePath = "http://localhost:8000";
        const apiConfig: Configuration = new Configuration({
            basePath,
        });
        this.api = new DefaultApi(apiConfig);
    }

    public async reset() {
        return await this.api.reset();
    }

    public async version() {
        const response = await this.api.version();
        return await handleSuccess(response);
    }

    public async heartbeat() {
        const response = await this.api.heartbeat();
        let ret = await handleSuccess(response);
        return ret["nanosecond heartbeat"]
    }

    public async persist() {
        throw new Error("Not implemented in JS client");
    }

    public async createCollection(
        name: string,
        metadata?: object,
        embeddingFunction?: IEmbeddingFunction
    ) {
        const newCollection = await this.api
            .createCollection({
                name,
                metadata,
            })
            .then(handleSuccess)
            .catch(handleError);

        if (newCollection.error) {
            throw new Error(newCollection.error);
        }

        return new Collection(name, newCollection.id, this.api, metadata, embeddingFunction);
    }

    public async getOrCreateCollection(
        name: string,
        metadata?: object,
        embeddingFunction?: IEmbeddingFunction
    ) {
        const newCollection = await this.api
            .createCollection({
                name,
                metadata,
                'get_or_create': true
            })
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

    public async listCollections() {
        const response = await this.api.listCollections();
        return handleSuccess(response);
    }

    public async getCollection(
        name: string,
        embeddingFunction?: IEmbeddingFunction
    ) {
        const response = await this.api
            .getCollection(name)
            .then(handleSuccess)
            .catch(handleError);

        return new Collection(
            response.name,
            response.id,
            this.api,
            response.metadata,
            embeddingFunction
        );
    }

    public async deleteCollection(name: string) {
        return await this.api
            .deleteCollection(name)
            .then(handleSuccess)
            .catch(handleError);
    }

}
