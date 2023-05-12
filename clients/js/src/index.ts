import {
  IncludeEnum,
} from "./types";
import { Configuration, ApiApi as DefaultApi, Api } from "./generated";
import Count200Response = Api.Count200Response;

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

// we need to override constructors to make it work with jest
// https://stackoverflow.com/questions/76007003/jest-tobeinstanceof-expected-constructor-array-received-constructor-array
function repack(value: unknown): any {
  if (Boolean(value) && typeof value === "object") {
    if (Array.isArray(value)) {
      return new Array(...value);
    } else {
      return { ...value };
    }
  } else {
    return value;
  }
}

async function handleError(error: unknown) {
  if (error instanceof Response) {
    try {
      const res = await error.json();
      if ("error" in res) {
        return { error: res.error };
      }
    } catch (e: unknown) {
      return {
        //@ts-ignore
        error:
          e && typeof e === "object" && "message" in e
            ? e.message
            : "unknown error",
      };
    }
  }
  return { error };
}

async function handleSuccess(response: Response | string | Count200Response) {
  switch (true) {
    case response instanceof Response:
      return repack(await (response as Response).json());
    case typeof response === "string":
      return repack((response as string)); // currently version is the only thing that return non-JSON
    default:
      return repack(response);
  }
}

class EmbeddingFunction { }

let OpenAIApi: any;

export class OpenAIEmbeddingFunction {
  private api_key: string;
  private org_id: string;
  private model: string;

  constructor(
    openai_api_key: string,
    openai_model?: string,
    openai_organization_id?: string
  ) {
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      OpenAIApi = require("openai");
    } catch {
      throw new Error(
        "Please install the openai package to use the OpenAIEmbeddingFunction, `npm install -S openai`"
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
      model: this.model,
      input: texts,
    });
    const data = response.data["data"];
    for (let i = 0; i < data.length; i += 1) {
      embeddings.push(data[i]["embedding"]);
    }
    return embeddings;
  }
}

let CohereAiApi: any;

export class CohereEmbeddingFunction {
  private api_key: string;
  private model: string;

  constructor(cohere_api_key: string, model?: string) {
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      CohereAiApi = require("cohere-ai");
    } catch {
      throw new Error(
        "Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`"
      );
    }
    this.api_key = cohere_api_key;
    this.model = model || "large";
  }

  public async generate(texts: string[]) {
    const cohere = CohereAiApi.init(this.api_key);
    const embeddings = [];
    const response = await CohereAiApi.embed({
      texts: texts,
      model: this.model,
    });
    return response.body.embeddings;
  }
}

type CallableFunction = {
  generate(texts: string[]): Promise<number[][]>;
};

export class Collection {
  public name: string;
  public id: string;
  public metadata: object | undefined;
  private api: DefaultApi;
  public embeddingFunction: CallableFunction | undefined;

  constructor(
    name: string,
    id: string,
    api: DefaultApi,
    metadata?: object,
    embeddingFunction?: CallableFunction
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
    embeddingFunction?: CallableFunction
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
    embeddingFunction?: CallableFunction
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
    embeddingFunction?: CallableFunction
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
