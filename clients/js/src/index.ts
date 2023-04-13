import {Configuration, DefaultApi} from "./generated";

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

class EmbeddingFunction { }

let OpenAIApi: any;

export class OpenAIEmbeddingFunction {
  private api_key: string;
  private org_id: string;
  private model: string;

  constructor(openai_api_key: string, openai_model?: string, openai_organization_id?: string) {
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      OpenAIApi = require("openai");
    } catch {
      throw new Error(
        "Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai`",
      );
    }
    this.api_key = openai_api_key;
    this.org_id = openai_organization_id || "";
    this.model = openai_model || "text-embedding-ada-002";
  }

  public async generate(texts: string[]): Promise<number[][]> {
    const configuration = new OpenAIApi.Configuration({
      organization: this.org_id,
      apiKey: this.api_key,
    });
    const openai = new OpenAIApi.OpenAIApi(configuration);
    const embeddings = [];
    const response = await openai.createEmbedding({
      model: "text-embedding-ada-002",
      input: texts,
    });
    const data = response.data['data'];
    for (let i = 0; i < data.length; i += 1) {
      embeddings.push(data[i]['embedding']);
    }
    return embeddings;
  }
}

let CohereAiApi: any;

export class CohereEmbeddingFunction {
  private api_key: string;

  constructor(cohere_api_key: string) {
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      CohereAiApi = require("cohere-ai");
    } catch {
      throw new Error(
        "Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`",
      );
    }
    this.api_key = cohere_api_key;
  }

  public async generate(texts: string[]) {
    const cohere = CohereAiApi.init(this.api_key);
    const embeddings = [];
    const response = await CohereAiApi.embed({
      texts: texts,
    });
    return response.body.embeddings;
  }
}

type CallableFunction = {
  generate(texts: string[]): Promise<number[][]>;
};

// we need to override constructors to make it work with jest
// https://stackoverflow.com/questions/76007003/jest-tobeinstanceof-expected-constructor-array-received-constructor-array
function repack(value: unknown): any {
  if(Boolean(value) && typeof value === 'object') {
    if (Array.isArray(value)) {
      return new Array(...value);
    } else {
      return {...value};
    }
  } else {
    return value;
  }
}

export class Collection {
  public name: string;
  private api: DefaultApi;
  public embeddingFunction: CallableFunction | undefined;

  constructor(name: string, api: DefaultApi, embeddingFunction?: CallableFunction) {
    this.name = name;
    this.api = api;
    if (embeddingFunction !== undefined)
      this.embeddingFunction = embeddingFunction;
  }

  public async add(
    ids: string | string[],
    embeddings: number[] | number[][] | undefined,
    metadatas?: object | object[],
    documents?: string | string[],
    increment_index: boolean = true,
  ) {
    if ((embeddings === undefined) && (documents === undefined)) {
      throw new Error(
        "embeddings and documents cannot both be undefined",
      );
    } else if ((embeddings === undefined) && (documents !== undefined)) {
      const documentsArray = toArray(documents);
      if (this.embeddingFunction !== undefined) {
        embeddings = await this.embeddingFunction.generate(documentsArray)
      } else {
        throw new Error(
          "embeddingFunction is undefined. Please configure an embedding function",
        );
      }
    }
    if (embeddings === undefined) throw new Error("embeddings is undefined but shouldnt be")

    const idsArray = toArray(ids);
    const embeddingsArray: number[][] = toArrayOfArrays(embeddings);

    let metadatasArray: object[] | undefined;
    if (metadatas === undefined) {
      metadatasArray = undefined
    } else {
      metadatasArray = toArray(metadatas);
    }

    let documentsArray: (string | undefined)[] | undefined;
    if (documents === undefined) {
      documentsArray = undefined
    } else {
      documentsArray = toArray(documents);
    }

    if (
      ((embeddingsArray !== undefined) && idsArray.length !== embeddingsArray.length) ||
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
    }).then(function (response) {
      return response.json()
    }).catch(function ({response}) {
      return response.data;
    })
  }

  public async count() {
    const response = await this.api.count({ collectionName: this.name });
      return response.json()
  }

  public async get(
    ids?: string[],
    where?: object,
    limit?: number,
    offset?: number,
  ) {
    let idsArray = undefined
    if (ids !== undefined) idsArray = toArray(ids);

    return await this.api.get({
      collectionName: this.name,
      getEmbedding: {
        ids: idsArray,
        where,
        limit,
        offset,
      },
    }).then(async function (response) {
      return repack(await response.json())
    }).catch(function ({response}) {
      return response.data;
    })

  }

  public async query(
    query_embeddings: number[] | number[][] | undefined,
    n_results: number = 10,
    where?: object,
    query_text?: string | string[],
  ) {
    if ((query_embeddings === undefined) && (query_text === undefined)) {
      throw new Error(
        "query_embeddings and query_text cannot both be undefined",
      );
    } else if ((query_embeddings === undefined) && (query_text !== undefined)) {
      const query_texts = toArray(query_text);
      if (this.embeddingFunction !== undefined) {
        query_embeddings = await this.embeddingFunction.generate(query_texts)
      } else {
        throw new Error(
          "embeddingFunction is undefined. Please configure an embedding function",
        );
      }
    }
    if (query_embeddings === undefined) throw new Error("embeddings is undefined but shouldnt be")

    const query_embeddingsArray: number[][] = toArrayOfArrays(query_embeddings);

    return await this.api.getNearestNeighbors({
      collectionName: this.name,
      queryEmbedding: {
        query_embeddings: query_embeddingsArray,
        where,
        n_results,
      },
    }).then(async function (response) {
      return repack(await response.json())
    }).catch(function ({response}) {
      return response.data;
    });
  }

  public async peek(limit: number = 10) {
    const response = await this.api.get({
      collectionName: this.name,
      getEmbedding: { limit: limit },
    });
      const data =await response.json();
      return repack(data);
  }

  public async createIndex() {
    return await this.api.createIndex({ collectionName: this.name });
  }

  public async delete(ids?: string[], where?: object) {
    return await this.api._delete({
      collectionName: this.name,
      deleteEmbedding: {ids: ids, where: where},
    }).then(function (response) {
      return response.json()
    }).catch(function ({response}) {
      return response.data;
    })
  }

}

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

  public async createCollection(name: string, metadata?: object, embeddingFunction?: CallableFunction) {
    const newCollection = await this.api.createCollection({
      createCollection: { name, metadata },
    }).then(async function (response) {
            const data = await response.json();
            return repack(data);
    }).catch(function ({ response }) {
      return response.data;
    });

    // @ts-ignore
    if (newCollection.error) {
      // @ts-ignore
      throw new Error(newCollection.error);
    }

    return new Collection(name, this.api, embeddingFunction);
  }

  public async listCollections() {
    const response = await this.api.listCollections();
    const data = await response.json();
    return Array.isArray(data) ? new Array(...data) : data;
  }

  public async getCollection(name: string, embeddingFunction?: CallableFunction) {
    return new Collection(name, this.api, embeddingFunction);
  }

  public async deleteCollection(name: string) {
    return await this.api.deleteCollection({collectionName: name}).then(function (response) {
      return response.json()
    }).catch(function ({response}) {
      return response.data;
    })
  }

}
