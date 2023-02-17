import { DefaultApi } from "./generated/api";
import { Configuration } from "./generated/configuration";

export class Collection {
  private name: string;
  private api: DefaultApi;

  constructor(name: string, api: DefaultApi) {
    this.name = name;
    this.api = api;
  }

  public async add(
    ids: string, 
    embeddings: number[], // this should be optional
    metadatas?: object,
    documents?: string | Array<any>,
    increment_index: boolean = true,
    ) {
    return await this.api.add({
      collectionName: this.name,
      addEmbedding: {
        ids,
        embeddings,
        documents,
        metadatas,
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
    return await this.api.get({
      collectionName: this.name,
      getEmbedding: { 
        ids, 
        where,
        limit,
        offset,
      },
    });
  }

  public async query(
    n_results: number = 10,
    query_embeddings: number[], 
    query_texts?: string[],
    where?: object,
    ) {
    const response = await this.api.getNearestNeighbors({
      collectionName: this.name,
      queryEmbedding: { 
        query_embeddings,
        query_texts,
        where,
        n_results,
      },
    });
    return response.data;
  }

  public async peek() {
    return await this.api.get({
      collectionName: this.name,
      getEmbedding: { limit: 10 },
    });
  }

  public async createIndex() {
    return await this.api.createIndex({ collectionName: this.name });
  }

  // public async modify(new_name: string) {
  //   const collection =  await this.api.updateCollection({
  //     collectionName: this.name,
  //     updateCollection: { new_name },
  //   });
  //   this.name = new_name;
  //   return this
  // }

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

export const chromaClient = (basePath: string): ChromaClient => {
  return new ChromaClient(basePath);
}

export default chromaClient;