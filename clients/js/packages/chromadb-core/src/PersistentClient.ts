import { PersistentCollection } from "./PersistentCollection";
import { DefaultEmbeddingFunction } from "./embeddings/DefaultEmbeddingFunction";
import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";
import type {
  CollectionMetadata,
  CreateCollectionParams,
  DeleteCollectionParams,
  GetCollectionParams,
  GetOrCreateCollectionParams,
  ListCollectionsParams,
} from "./types";

const DEFAULT_TENANT = "default_tenant";
const DEFAULT_DATABASE = "default_database";

export interface PersistentClientParams {
  path: string;
  tenant?: string;
  database?: string;
  allowReset?: boolean;
}

interface NativeBindings {
  new (config: { persistPath: string; allowReset?: boolean }): NativeBindings;
  heartbeat(): number;
  createCollection(
    name: string,
    tenant: string | undefined,
    database: string | undefined,
  ): { id: string; name: string; tenant: string; database: string };
  getOrCreateCollection(
    name: string,
    tenant: string | undefined,
    database: string | undefined,
  ): { id: string; name: string; tenant: string; database: string };
  getCollection(
    name: string,
    tenant: string | undefined,
    database: string | undefined,
  ): { id: string; name: string; tenant: string; database: string };
  deleteCollection(
    name: string,
    tenant: string | undefined,
    database: string | undefined,
  ): void;
  listCollections(
    tenant: string | undefined,
    database: string | undefined,
  ): { id: string; name: string; tenant: string; database: string }[];
  add(
    collectionId: string,
    ids: string[],
    embeddings: number[][],
    metadatasJson: string | null,
    documentsJson: string | null,
    tenant: string | undefined,
    database: string | undefined,
  ): boolean;
  query(
    collectionId: string,
    queryEmbeddings: number[][],
    nResults: number,
    tenant: string | undefined,
    database: string | undefined,
    include: string[] | undefined,
  ): {
    ids: string[][];
    embeddings: string | null;
    documents: string | null;
    metadatas: string | null;
    distances: number[][] | null;
  };
  get(
    collectionId: string,
    ids: string[] | undefined,
    limit: number | undefined,
    offset: number | undefined,
    tenant: string | undefined,
    database: string | undefined,
    include: string[] | undefined,
  ): {
    ids: string[];
    embeddings: number[][] | null;
    documents: string | null;
    metadatas: string | null;
  };
  count(
    collectionId: string,
    tenant: string | undefined,
    database: string | undefined,
  ): number;
  deleteRecords(
    collectionId: string,
    ids: string[] | undefined,
    tenant: string | undefined,
    database: string | undefined,
  ): void;
  reset(): boolean;
}

let _bindingsClass: (new (config: {
  persistPath: string;
  allowReset?: boolean;
}) => NativeBindings) | null = null;

function getBindingsClass(): new (config: {
  persistPath: string;
  allowReset?: boolean;
}) => NativeBindings {
  if (_bindingsClass) {
    return _bindingsClass;
  }

  try {
    // Try to load from chromadb-js-bindings package (when installed as dependency)
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const bindings = require("chromadb-js-bindings");
    _bindingsClass = bindings.Bindings;
    return _bindingsClass!;
  } catch {
    // Try platform-specific packages
    const platform = process.platform;
    const arch = process.arch;

    const packageMap: Record<string, string> = {
      "darwin-arm64": "chromadb-js-bindings-darwin-arm64",
      "darwin-x64": "chromadb-js-bindings-darwin-x64",
      "linux-arm64": "chromadb-js-bindings-linux-arm64-gnu",
      "linux-x64": "chromadb-js-bindings-linux-x64-gnu",
      "win32-x64": "chromadb-js-bindings-win32-x64-msvc",
    };

    const key = `${platform}-${arch}`;
    const packageName = packageMap[key];

    if (!packageName) {
      throw new Error(
        `PersistentClient is not supported on this platform: ${platform}-${arch}. ` +
          `Supported platforms: ${Object.keys(packageMap).join(", ")}`,
      );
    }

    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const bindings = require(packageName);
      _bindingsClass = bindings.Bindings;
      return _bindingsClass!;
    } catch (e) {
      throw new Error(
        `Failed to load native bindings. Please install the optional dependency: npm install ${packageName}\n` +
          `Original error: ${e}`,
      );
    }
  }
}

export class PersistentClient {
  private bindings: NativeBindings;
  public tenant: string;
  public database: string;

  constructor({
    path,
    tenant = DEFAULT_TENANT,
    database = DEFAULT_DATABASE,
    allowReset = false,
  }: PersistentClientParams) {
    this.tenant = tenant;
    this.database = database;

    const BindingsClass = getBindingsClass();
    this.bindings = new BindingsClass({
      persistPath: path,
      allowReset,
    });
  }

  async heartbeat(): Promise<number> {
    return this.bindings.heartbeat();
  }

  async reset(): Promise<boolean> {
    return this.bindings.reset();
  }

  async createCollection({
    name,
    metadata,
    embeddingFunction = new DefaultEmbeddingFunction(),
  }: CreateCollectionParams): Promise<PersistentCollection> {
    const collection = this.bindings.createCollection(
      name,
      this.tenant,
      this.database,
    );

    return new PersistentCollection(
      collection.name,
      collection.id,
      this,
      embeddingFunction,
      metadata,
    );
  }

  async getOrCreateCollection({
    name,
    metadata,
    embeddingFunction = new DefaultEmbeddingFunction(),
  }: GetOrCreateCollectionParams): Promise<PersistentCollection> {
    const collection = this.bindings.getOrCreateCollection(
      name,
      this.tenant,
      this.database,
    );

    return new PersistentCollection(
      collection.name,
      collection.id,
      this,
      embeddingFunction,
      metadata,
    );
  }

  async getCollection({
    name,
    embeddingFunction = new DefaultEmbeddingFunction(),
  }: GetCollectionParams): Promise<PersistentCollection> {
    const collection = this.bindings.getCollection(
      name,
      this.tenant,
      this.database,
    );

    return new PersistentCollection(
      collection.name,
      collection.id,
      this,
      embeddingFunction,
      undefined,
    );
  }

  async deleteCollection({ name }: DeleteCollectionParams): Promise<void> {
    this.bindings.deleteCollection(name, this.tenant, this.database);
  }

  async listCollections({ limit, offset }: ListCollectionsParams = {}): Promise<
    string[]
  > {
    const collections = (this.bindings as any).listCollections(
      this.tenant,
      this.database,
      limit,
      offset,
    );
    return collections.map((c: { name: string }) => c.name);
  }

  async listCollectionsAndMetadata({
    limit,
    offset,
  }: ListCollectionsParams = {}): Promise<
    { name: string; id: string; metadata?: CollectionMetadata }[]
  > {
    const collections = (this.bindings as any).listCollections(
      this.tenant,
      this.database,
      limit,
      offset,
    );
    return collections.map((c: { name: string; id: string }) => ({
      name: c.name,
      id: c.id,
      metadata: undefined,
    }));
  }

  async countCollections(): Promise<number> {
    const collections = (this.bindings as any).listCollections(
      this.tenant,
      this.database,
    );
    return collections.length;
  }

  /** @internal - Used by PersistentCollection */
  _getBindings(): NativeBindings {
    return this.bindings;
  }
}
