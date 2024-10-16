import { IEmbeddingFunction } from "./IEmbeddingFunction";
interface LCEmbeddingsInterfaceProxy extends IEmbeddingFunction {
  embedDocuments(documents: string[]): Promise<number[][]>;
  embedQuery(document: string): Promise<number[]>;
}
export class LangChainEmbeddingFunction {
  static async create({
    langchainEmbeddings = null,
    chromaEmbeddingFunction = null,
  }: {
    langchainEmbeddings?: any;
    chromaEmbeddingFunction?: IEmbeddingFunction | null;
  } = {}): Promise<LCEmbeddingsInterfaceProxy> {
    if (!langchainEmbeddings && !chromaEmbeddingFunction) {
      throw new Error(
        "At least one of langchainEmbeddings or chromaEmbeddingFunction is required",
      );
    }
    if (langchainEmbeddings && chromaEmbeddingFunction) {
      throw new Error(
        "Only one of langchainEmbeddings or chromaEmbeddingFunction is allowed",
      );
    }

    // Dynamically import the classes from the library
    let Embeddings: any;
    try {
      const importedModule = await import("@langchain/core/embeddings");
      Embeddings = importedModule.Embeddings;
    } catch (e) {
      throw new Error(
        "The library '@langchain/core' is not installed. Please install it with 'npm install @langchain/core'",
      );
    }

    if (langchainEmbeddings) {
      // Check if the imported embeddings is an instance of Embeddings class
      if (!(langchainEmbeddings instanceof Embeddings)) {
        throw new Error("embeddings must be an instance of Embeddings");
      }

      // Ensure the embeddings object has the required methods (mimicking EmbeddingsInterface)
      if (
        typeof langchainEmbeddings.embedDocuments !== "function" ||
        typeof langchainEmbeddings.embedQuery !== "function"
      ) {
        throw new Error(
          "embeddings must implement embedDocuments and embedQuery methods",
        );
      }
    }
    if (chromaEmbeddingFunction) {
      if (typeof chromaEmbeddingFunction.generate !== "function") {
        throw new Error("Chroma embeddings must implement generate methods");
      }
    }

    // Return an instance of the dynamically created class
    return new (class extends Embeddings implements IEmbeddingFunction {
      protected langchainEmbeddings?: any;
      protected chromaEmbeddingFunction?: IEmbeddingFunction | null;
      constructor({
        langchainEmbeddings = null,
        chromaEmbeddingFunction = null,
      }: {
        langchainEmbeddings?: any;
        chromaEmbeddingFunction?: IEmbeddingFunction | null;
      } = {}) {
        super();
        if (!langchainEmbeddings && !chromaEmbeddingFunction) {
          throw new Error(
            "At least one of langchainEmbeddings or chromaEmbeddingFunction is required",
          );
        }
        if (langchainEmbeddings && chromaEmbeddingFunction) {
          throw new Error(
            "Only one of langchainEmbeddings or chromaEmbeddingFunction is allowed",
          );
        }
        this.langchainEmbeddings = langchainEmbeddings;
        this.chromaEmbeddingFunction = chromaEmbeddingFunction;
      }
      embedDocuments(documents: string[]): Promise<number[][]> {
        if (this.langchainEmbeddings) {
          return this.langchainEmbeddings.embedDocuments(documents);
        } else if (this.chromaEmbeddingFunction) {
          return this.chromaEmbeddingFunction.generate(documents);
        } else {
          throw new Error(
            "The wrapper does not have any configured embedding function.",
          );
        }
      }

      embedQuery(document: string): Promise<number[]> {
        if (this.langchainEmbeddings) {
          return this.langchainEmbeddings.embedQuery(document);
        } else if (this.chromaEmbeddingFunction) {
          return this.chromaEmbeddingFunction
            .generate([document])
            .then((embeddings) => embeddings[0]);
        } else {
          throw new Error(
            "The wrapper does not have any configured embedding function.",
          );
        }
      }

      async generate(texts: string[]): Promise<number[][]> {
        if (this.langchainEmbeddings) {
          return this.langchainEmbeddings.embedDocuments(texts);
        } else if (this.chromaEmbeddingFunction) {
          return this.chromaEmbeddingFunction.generate(texts);
        } else {
          throw new Error(
            "The wrapper does not have any configured embedding function.",
          );
        }
      }
    })({
      langchainEmbeddings: langchainEmbeddings,
      chromaEmbeddingFunction: chromaEmbeddingFunction,
    });
  }
}
