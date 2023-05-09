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

/**
 * Class for generating embeddings using OpenAI API.
 */
export class OpenAIEmbeddingFunction {
  private api_key: string;
  private org_id: string;
  private model: string;

  /**
   * Creates an instance of OpenAIEmbeddingFunction.
   * @param openai_api_key - The API key for OpenAI.
   * @param openai_model - (Optional) The model to use for generating embeddings. Defaults to 'text-embedding-ada-002'.
   * @param openai_organization_id - (Optional) The organization ID for the OpenAI service.
   */
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

  /**
   * Generates embeddings for the given texts using the OpenAI API.
   * @param texts - An array of texts to generate embeddings for.
   * @returns A Promise that resolves with the embeddings.
   */
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

/**
 * Class for generating embeddings using Cohere API.
 */
export class CohereEmbeddingFunction {
  private api_key: string;

  /**
   * Creates an instance of CohereEmbeddingFunction.
   * @param cohere_api_key - The API key for Cohere.
   */
  constructor(cohere_api_key: string) {
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      CohereAiApi = require("cohere-ai");
    } catch {
      throw new Error(
        "Please install the cohere-ai package to use the CohereEmbeddingFunction, `npm install -S cohere-ai`"
      );
    }
    this.api_key = cohere_api_key;
  }

  /**
   * Generates embeddings for the given texts using the Cohere API.
   * @param texts - An array of texts to generate embeddings for.
   * @returns A Promise that resolves with the embeddings.
   */
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

/**
 * Represents a collection of embeddings.
 */
export class Collection {
  public name: string;
  public metadata: object | undefined;
  private api: DefaultApi;
  public embeddingFunction: CallableFunction | undefined;

  /**
   * Creates a new Collection instance.
   * @param name - The name of the collection.
   * @param api - The API instance for the collection.
   * @param metadata - (Optional) Metadata for the collection.
   * @param embeddingFunction - (Optional) A callable function for generating embeddings.
   */
  constructor(
    name: string,
    api: DefaultApi,
    metadata?: object,
    embeddingFunction?: CallableFunction
  ) {
    this.name = name;
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

  /**
   * Adds items to the collection.
   * @param ids - A single ID or an array of IDs for the items to be added.
   * @param embeddings - A single embedding or an array of embeddings. If undefined, embeddings will be generated from documents.
   * @param metadatas - (Optional) A single metadata object or an array of metadata objects associated with the items.
   * @param documents - (Optional) A single document or an array of documents to store and generate embeddings for, if embeddings are not provided.
   * @returns A Promise that resolves to a Boolean indicating whether the items were added successfully.
   */
  public async add(
    ids: string | string[],
    embeddings: number[] | number[][] | undefined,
    metadatas?: object | object[],
    documents?: string | string[],
  ) {

    const [idsArray, embeddingsArray, metadatasArray, documentsArray] = await this.validate(
      true,
      ids,
      embeddings,
      metadatas,
      documents
    )

    const response = await this.api.add(this.name,
      {
        // @ts-ignore
        ids: idsArray,
        embeddings: embeddingsArray as number[][], // We know this is defined because of the validate function
        // @ts-ignore
        documents: documentsArray,
        metadatas: metadatasArray,
      })
      .then(handleSuccess)
      .catch(handleError);

    return response
  }

  /**
   * Upserts items in the collection.
   * @param ids - A single ID or an array of IDs for the items to be upserted.
   * @param embeddings - A single embedding or an array of embeddings. If undefined, embeddings will be generated from documents.
   * @param metadatas - (Optional) A single metadata object or an array of metadata objects associated with the items.
   * @param documents - (Optional) A single document or an array of documents to store and generate embeddings for, if embeddings are not provided.
   * @returns A Promise that resolves to a Boolean indicating whether the items were upserted successfully.
   * @remarks Upsert is a combination of add and update. If an item with the same ID already exists, it will be updated. Otherwise, it will be added.
  */
  public async upsert(
    ids: string | string[],
    embeddings: number[] | number[][] | undefined,
    metadatas?: object | object[],
    documents?: string | string[],
  ) {

    const [idsArray, embeddingsArray, metadatasArray, documentsArray] = await this.validate(
      true,
      ids,
      embeddings,
      metadatas,
      documents
    )

    const response = await this.api.upsert(this.name,
      {
        //@ts-ignore
        ids: idsArray,
        embeddings: embeddingsArray as number[][], // We know this is defined because of the validate function
        //@ts-ignore
        documents: documentsArray,
        metadatas: metadatasArray,
      },
    )
      .then(handleSuccess)
      .catch(handleError);

    return response

  }

  /**
   * Returns the number of items in the collection.
   * @returns A Promise that resolves to the number of items in the collection.
   */
  public async count() {
    const response = await this.api.count(this.name);
    return handleSuccess(response);
  }

  /**
   * Change the name or metadata of the collection.
   * @param name - (Optional) The new name for the collection.
   * @param metadata - (Optional) The new metadata for the collection.
   * @returns A Promise that resolves to a Boolean indicating whether the collection was modified successfully.
   * @remarks If the name or metadata is not provided, the existing name or metadata will be used.
  */
  public async modify(name?: string, metadata?: object) {
    const response = await this.api
      .updateCollection(
        this.name,
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

  /**
   * Retrieves items from the collection.
   * @param ids - (Optional) An array of IDs of the items to be retrieved.
   * @param where - (Optional) An object specifying filtering conditions.
   * @param limit - (Optional) A number indicating the maximum number of items to retrieve.
   * @param offset - (Optional) A number indicating the offset for pagination.
   * @param include - (Optional) An array of strings specifying which fields to include in the response.
   * @param where_document - (Optional) An object specifying filtering conditions for the document field.
   * @returns A Promise that resolves to the retrieved items.
   */
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
      .aGet(this.name, {
        ids: idsArray,
        where,
        limit,
        offset,
        include,
        where_document
      })
      .then(handleSuccess)
      .catch(handleError);
  }

  /**
   * Updates items in the collection.
   * @param ids - A single ID or an array of IDs for the items to be updated.
   * @param embeddings - (Optional) A single embedding or an array of embeddings. If undefined, embeddings will be generated from documents.
   * @param metadatas - (Optional) A single metadata object or an array of metadata objects associated with the items.
   * @param documents - (Optional) A single document or an array of documents to store and generate embeddings for, if embeddings are not provided.
   * @returns A Promise that resolves to a Boolean indicating whether the items were updated successfully.
   */
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
        this.name,
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

  /**
   * Queries the collection for the nearest neighbors of the provided query embeddings or query texts.
   * @param query_embeddings - A single query embedding or an array of query embeddings. If undefined, embeddings will be generated from query_text.
   * @param n_results - (Optional) The number of nearest neighbors to return. Default is 10.
   * @param where - (Optional) An object specifying filtering conditions.
   * @param query_text - (Optional) A single query text or an array of query texts to generate embeddings for, if query_embeddings are not provided.
   * @param where_document - (Optional) An object specifying filtering conditions for the document field.
   * @param include - (Optional) An array of strings specifying which fields to include in the response.
   * @returns A Promise that resolves to the nearest neighbors of the query embeddings or query texts.
   */
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
      .getNearestNeighbors(this.name, {
        query_embeddings: query_embeddingsArray,
        where,
        n_results: n_results,
        where_document: where_document,
        include: include,
      })
      .then(handleSuccess)
      .catch(handleError);
  }

  /**
   * Retrieves a limited number of items from the collection.
   * @param limit - (Optional) The maximum number of items to retrieve. Default is 10.
   * @returns A Promise that resolves to the retrieved items.
   */
  public async peek(limit: number = 10) {
    const response = await this.api.aGet(this.name, {
      limit: limit,
    });
    return handleSuccess(response);
  }

  public async createIndex() {
    return await this.api.createIndex(this.name);
  }

  /**
   * Deletes items from the collection.
   * @param ids - (Optional) An array of IDs of the items to be deleted.
   * @param where - (Optional) An object specifying filtering conditions for deletion.
   * @param where_document - (Optional) An object specifying filtering conditions for the document field.
   * @returns A Promise that resolves to the result of the deletion operation.
   */
  public async delete(ids?: string[], where?: object, where_document?: object) {
    return await this.api
      .aDelete(this.name, { ids: ids, where: where, where_document: where_document })
      .then(handleSuccess)
      .catch(handleError);
  }
}

/**
 * Represents a ChromaClient for managing collections of embeddings.
 */
export class ChromaClient {
  private api: DefaultApi;

  /**
   * Creates a new ChromaClient instance.
   * @param basePath - (Optional) The base URL of the Chroma API. Default is "http://localhost:8000".
   */
  constructor(basePath?: string) {
    if (basePath === undefined) basePath = "http://localhost:8000";
    const apiConfig: Configuration = new Configuration({
      basePath,
    });
    this.api = new DefaultApi(apiConfig);
  }

  /**
   * Resets the ChromaClient state.
   * @returns A Promise that resolves when the ChromaClient state is reset.
   */
  public async reset() {
    return await this.api.reset();
  }

  /**
   * Returns the version of the Chroma API.
   * @returns A Promise that resolves to the version of the Chroma API.
  */
  public async version() {
    const response = await this.api.version();
    return await handleSuccess(response);
  }

  /**
   * Returns the heartbeat of the Chroma API.
   * @returns A Promise that resolves to the heartbeat of the Chroma API.
  */
  public async heartbeat() {
    const response = await this.api.heartbeat();
    let ret = await handleSuccess(response);
    return ret["nanosecond heartbeat"]
  }

  public async persist() {
    throw new Error("Not implemented in JS client");
  }

  /**
   * Creates a new collection.
   * @param name - The name of the new collection.
   * @param metadata - (Optional) An object containing metadata for the new collection.
   * @param embeddingFunction - (Optional) A callable function for generating embeddings.
   * @returns A Promise that resolves to the created Collection instance.
   */
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

    return new Collection(name, this.api, metadata, embeddingFunction);
  }

  /**
   * Retrieves an existing collection or creates a new one if it does not exist.
   * @param name - The name of the collection to be retrieved or created.
   * @param metadata - (Optional) An object containing metadata for the new collection.
   * @param embeddingFunction - (Optional) A callable function for generating embeddings.
   * @returns A Promise that resolves to the retrieved or created Collection instance.
   * @remarks This method is useful for ensuring that a collection exists before attempting to add items to it.
  */
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
      this.api,
      newCollection.metadata,
      embeddingFunction
    );
  }

  /**
   * Lists all collections.
   * @returns A Promise that resolves to an array of collections.
   */
  public async listCollections() {
    const response = await this.api.listCollections();
    return handleSuccess(response);
  }

  /**
   * Retrieves an existing collection.
   * @param name - The name of the collection to be retrieved.
   * @param embeddingFunction - (Optional) A callable function for generating embeddings.
   * @returns A Collection instance representing the retrieved collection.
   */
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
      this.api,
      response.metadata,
      embeddingFunction
    );
  }

  /**
   * Deletes a collection.
   * @param name - The name of the collection to be deleted.
   * @returns A Promise that resolves to the result of the deletion operation.
   */
  public async deleteCollection(name: string) {
    return await this.api
      .deleteCollection(name)
      .then(handleSuccess)
      .catch(handleError);
  }
}
