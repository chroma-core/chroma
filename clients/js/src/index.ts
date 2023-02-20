import { DefaultApi } from "./generated/api";
import { Configuration } from "./generated/configuration";

// a function to convert a non-Array object to an Array
function toArray<T>(obj: T | Array<T>): Array<T> {
  if (Array.isArray(obj)) {
    return obj;
  } else {
    return [obj];
  }
}

// a function to convert an array to array of arrays
function toArrayOfArrays<T>(obj: Array<Array<T>> | Array<T>): Array<Array<T>> {
  if (Array.isArray(obj[0])) {
    return obj as Array<Array<T>>;
  } else {
    return [obj] as Array<Array<T>>;
  }
}

export class Collection {
  private name: string;
  private api: DefaultApi;

  constructor(name: string, api: DefaultApi) {
    this.name = name;
    this.api = api;
  }

  public async add(
    ids: string | Array<any>,
    embeddings: Array<any>,
    metadatas?: Array<any> | object,
    documents?: string | Array<any>,
    increment_index: boolean = true,
  ) {

    const idsArray = toArray(ids);
    const embeddingsArray = toArrayOfArrays(embeddings);

    let metadatasArray;
    if (metadatas === undefined) {
      metadatasArray = undefined
    } else {
      metadatasArray = toArray(metadatas);
    }

    let documentsArray;
    if (metadatas === undefined) {
      documentsArray = undefined
    } else {
      documentsArray = toArray(metadatas);
    }

    if (
      idsArray.length !== embeddingsArray.length ||
      ((metadatasArray !== undefined) && idsArray.length !== metadatasArray.length) ||
      ((documentsArray !== undefined) && idsArray.length !== documentsArray.length)
    ) {
      throw new Error(
        "ids, embeddings, metadatas, and documents must all be the same length",
      );
    }

    return await this.api.add({
      collectionName: this.name,
      addEmbedding: {
        ids: idsArray,
        embeddings: embeddingsArray,
        documents: documentsArray,
        metadatas: metadatasArray,
        increment_index: increment_index,
      },
    });
  }

  public async count() {
    const response = await this.api.count({ collectionName: this.name });
    return response.data;
  }

  public async get(
    ids?: string[],
    where?: object,
    limit?: number,
    offset?: number,
  ) {
    const idsArray = toArray(ids);

    return await this.api.get({
      collectionName: this.name,
      getEmbedding: {
        ids: idsArray,
        where,
        limit,
        offset,
      },
    });
  }

  public async query(
    query_embeddings: number[],
    n_results: number = 10,
    where?: object,
  ) {
    const query_embeddingsArray = toArrayOfArrays(query_embeddings);

    const response = await this.api.getNearestNeighbors({
      collectionName: this.name,
      queryEmbedding: {
        query_embeddings: query_embeddingsArray,
        where,
        n_results,
      },
    });
    return response.data;
  }

  public async peek(limit: number = 10) {
    return await this.api.get({
      collectionName: this.name,
      getEmbedding: { limit: limit },
    });
  }

  public async createIndex() {
    return await this.api.createIndex({ collectionName: this.name });
  }

  public async delete(ids?: string[], where?: object) {
    return await this.api._delete({
      collectionName: this.name,
      deleteEmbedding: { ids: ids, where: where },
    });
  }

}

export class ChromaClient {
  private api: DefaultApi;

  constructor(basePath: string) {
    const apiConfig: Configuration = new Configuration({
      basePath,
    });
    this.api = new DefaultApi(apiConfig);
  }

  public async reset() {
    return await this.api.reset();
  }

  public async createCollection(name: string, metadata?: object) {
    const newCollection = await this.api.createCollection({
      createCollection: { name, metadata },
    });
    return new Collection(name, this.api);
  }

  public async listCollections() {
    const response = await this.api.listCollections();
    return response.data;
  }

  public async getCollection(name: string) {
    return new Collection(name, this.api);
  }

  public async deleteCollection(name: string) {
    return await this.api.deleteCollection({ collectionName: name });
  }

}