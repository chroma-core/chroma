import { IEmbeddingFunction } from './embeddings/IEmbeddingFunction';
import { ApiApi as DefaultApi } from "./generated";
import { toArray, toArrayOfArrays, handleError, handleSuccess, } from './utils';
import { IncludeEnum } from "./types";

export class Collection {
    public name: string;
    public id: string;
    public metadata: object | undefined;
    private api: DefaultApi;
    public embeddingFunction: IEmbeddingFunction | undefined;

    constructor(
        name: string,
        id: string,
        api: DefaultApi,
        metadata?: object,
        embeddingFunction?: IEmbeddingFunction
    ) {
        this.name = name;
        this.id = id;
        this.metadata = metadata;
        this.api = api;
        if (embeddingFunction !== undefined)
            this.embeddingFunction = embeddingFunction;
    }

    private setName(name: string) {
        this.name = name;
    }
    private setMetadata(metadata: object | undefined) {
        this.metadata = metadata;
    }

    private async validate(
        require_embeddings_or_documents: boolean, // set to false in the case of Update
        ids: string | string[],
        embeddings: number[] | number[][] | undefined,
        metadatas?: object | object[],
        documents?: string | string[],
    ) {

        if (require_embeddings_or_documents) {
            if ((embeddings === undefined) && (documents === undefined)) {
                throw new Error(
                    "embeddings and documents cannot both be undefined",
                );
            }
        }

        if ((embeddings === undefined) && (documents !== undefined)) {
            const documentsArray = toArray(documents);
            if (this.embeddingFunction !== undefined) {
                embeddings = await this.embeddingFunction.generate(documentsArray);
            } else {
                throw new Error(
                    "embeddingFunction is undefined. Please configure an embedding function"
                );
            }
        }
        if (embeddings === undefined)
            throw new Error("embeddings is undefined but shouldnt be");

        const idsArray = toArray(ids);
        const embeddingsArray: number[][] = toArrayOfArrays(embeddings);

        let metadatasArray: object[] | undefined;
        if (metadatas === undefined) {
            metadatasArray = undefined;
        } else {
            metadatasArray = toArray(metadatas);
        }

        let documentsArray: (string | undefined)[] | undefined;
        if (documents === undefined) {
            documentsArray = undefined;
        } else {
            documentsArray = toArray(documents);
        }

        if (
            (embeddingsArray !== undefined &&
                idsArray.length !== embeddingsArray.length) ||
            (metadatasArray !== undefined &&
                idsArray.length !== metadatasArray.length) ||
            (documentsArray !== undefined &&
                idsArray.length !== documentsArray.length)
        ) {
            throw new Error(
                "ids, embeddings, metadatas, and documents must all be the same length"
            );
        }

        const uniqueIds = new Set(idsArray);
        if (uniqueIds.size !== idsArray.length) {
            const duplicateIds = idsArray.filter((item, index) => idsArray.indexOf(item) !== index);
            throw new Error(
                `Expected IDs to be unique, found duplicates for: ${duplicateIds}`,
            );
        }

        return [idsArray, embeddingsArray, metadatasArray, documentsArray]
    }

    public async add(
        ids: string | string[],
        embeddings: number[] | number[][] | undefined,
        metadatas?: object | object[],
        documents?: string | string[],
        increment_index: boolean = true,
    ) {

        const [idsArray, embeddingsArray, metadatasArray, documentsArray] = await this.validate(
            true,
            ids,
            embeddings,
            metadatas,
            documents
        )

        const response = await this.api.add(this.id,
            {
                // @ts-ignore
                ids: idsArray,
                embeddings: embeddingsArray as number[][], // We know this is defined because of the validate function
                // @ts-ignore
                documents: documentsArray,
                metadatas: metadatasArray,
                incrementIndex: increment_index,
            })
            .then(handleSuccess)
            .catch(handleError);

        return response
    }

    public async upsert(
        ids: string | string[],
        embeddings: number[] | number[][] | undefined,
        metadatas?: object | object[],
        documents?: string | string[],
        increment_index: boolean = true,
    ) {

        const [idsArray, embeddingsArray, metadatasArray, documentsArray] = await this.validate(
            true,
            ids,
            embeddings,
            metadatas,
            documents
        )

        const response = await this.api.upsert(this.id,
            {
                //@ts-ignore
                ids: idsArray,
                embeddings: embeddingsArray as number[][], // We know this is defined because of the validate function
                //@ts-ignore
                documents: documentsArray,
                metadatas: metadatasArray,
                increment_index: increment_index,
            },
        )
            .then(handleSuccess)
            .catch(handleError);

        return response

    }


    public async count() {
        const response = await this.api.count(this.id);
        return handleSuccess(response);
    }

    public async modify(name?: string, metadata?: object) {
        const response = await this.api
            .updateCollection(
                this.id,
                {
                    new_name: name,
                    new_metadata: metadata,
                },
            )
            .then(handleSuccess)
            .catch(handleError);

        this.setName(name || this.name);
        this.setMetadata(metadata || this.metadata);

        return response;

    }

    public async get(
        ids?: string[],
        where?: object,
        limit?: number,
        offset?: number,
        include?: IncludeEnum[],
        where_document?: object
    ) {
        let idsArray = undefined;
        if (ids !== undefined) idsArray = toArray(ids);

        return await this.api
            .aGet(this.id, {
                ids: idsArray,
                where,
                limit,
                offset,
                include,
            })
            .then(handleSuccess)
            .catch(handleError);
    }

    public async update(
        ids: string | string[],
        embeddings?: number[] | number[][],
        metadatas?: object | object[],
        documents?: string | string[]
    ) {
        if (
            embeddings === undefined &&
            documents === undefined &&
            metadatas === undefined
        ) {
            throw new Error(
                "embeddings, documents, and metadatas cannot all be undefined"
            );
        } else if (embeddings === undefined && documents !== undefined) {
            const documentsArray = toArray(documents);
            if (this.embeddingFunction !== undefined) {
                embeddings = await this.embeddingFunction.generate(documentsArray);
            } else {
                throw new Error(
                    "embeddingFunction is undefined. Please configure an embedding function"
                );
            }
        }

        var resp = await this.api
            .update(
                this.id,
                {
                    ids: toArray(ids),
                    embeddings: embeddings ? toArrayOfArrays(embeddings) : undefined,
                    documents: documents, //TODO: this was toArray(documents) but that was wrong?
                    metadatas: toArray(metadatas),
                },
            )
            .then(handleSuccess)
            .catch(handleError);

        return resp;
    }

    public async query(
        query_embeddings: number[] | number[][] | undefined,
        n_results: number = 10,
        where?: object,
        query_text?: string | string[], // TODO: should be named query_texts to match python API
        where_document?: object, // {"$contains":"search_string"}
        include?: IncludeEnum[] // ["metadata", "document"]
    ) {
        if (query_embeddings === undefined && query_text === undefined) {
            throw new Error(
                "query_embeddings and query_text cannot both be undefined"
            );
        } else if (query_embeddings === undefined && query_text !== undefined) {
            const query_texts = toArray(query_text);
            if (this.embeddingFunction !== undefined) {
                query_embeddings = await this.embeddingFunction.generate(query_texts);
            } else {
                throw new Error(
                    "embeddingFunction is undefined. Please configure an embedding function"
                );
            }
        }
        if (query_embeddings === undefined)
            throw new Error("embeddings is undefined but shouldnt be");

        const query_embeddingsArray: number[][] = toArrayOfArrays(query_embeddings);

        return await this.api
            .getNearestNeighbors(this.id, {
                query_embeddings: query_embeddingsArray,
                where,
                n_results: n_results,
                where_document: where_document,
                include: include,
            })
            .then(handleSuccess)
            .catch(handleError);
    }

    public async peek(limit: number = 10) {
        const response = await this.api.aGet(this.id, {
            limit: limit,
        });
        return handleSuccess(response);
    }

    public async createIndex() {
        return await this.api.createIndex(this.name);
    }

    public async delete(ids?: string[], where?: object, where_document?: object) {
        return await this.api
            .aDelete(this.id, { ids: ids, where: where, where_document: where_document })
            .then(handleSuccess)
            .catch(handleError);
    }
}

