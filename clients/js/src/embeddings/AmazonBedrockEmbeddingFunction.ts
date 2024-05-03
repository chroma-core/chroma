import { IEmbeddingFunction } from "./IEmbeddingFunction";

let amazonBedrockApi: any;

export class AmazonBedrockEmbeddingFunction implements IEmbeddingFunction {
  private model: string;
  private configuration: any; // Assume this is BedrockRuntimeClientConfig
  private amazonBedrockApi?: any;
  private invokeCommand?: any;

  constructor({ config, model }: { config: any; model?: string }) {
    this.configuration = config;
    if (!this.configuration.region) {
      this.configuration.region = "us-east-1";
    }
    this.model = model || "amazon.titan-embed-text-v1";
  }

  private async loadClient() {
    if (this.amazonBedrockApi) return;
    try {
      // eslint-disable-next-line global-require,import/no-extraneous-dependencies
      const { amazonBedrock } = await AmazonBedrockEmbeddingFunction.import();
      amazonBedrockApi = new amazonBedrock.BedrockRuntimeClient(
        this.configuration,
      );
      this.invokeCommand = amazonBedrock.InvokeModelCommand;
    } catch (_a) {
      // @ts-ignore
      if (_a.code === "MODULE_NOT_FOUND") {
        throw new Error(
          "Please install the @aws-sdk/client-bedrock-runtime package to use the AmazonBedrockEmbeddingFunction, `npm install -S @aws-sdk/client-bedrock-runtime`",
        );
      }
      throw _a; // Re-throw other errors
    }
    this.amazonBedrockApi = amazonBedrockApi;
  }

  public async generate(texts: string[]): Promise<number[][]> {
    await this.loadClient();
    const td = new TextDecoder();
    const embeddings: number[][] = [];
    return Promise.all(
      texts.map(async (text) => {
        const input = {
          modelId: this.model,
          contentType: "application/json",
          accept: "application/json",
          body: JSON.stringify({ inputText: text }),
        };
        const command = new this.invokeCommand(input);
        const response = await this.amazonBedrockApi.send(command);
        const parsedBody = JSON.parse(td.decode(response.body));
        return parsedBody.embedding;
      }),
    );
  }

  /** @ignore */
  static async import(): Promise<{
    // @ts-ignore
    amazonBedrock: typeof import("@aws-sdk/client-bedrock-runtime");
  }> {
    try {
      // @ts-ignore
      const amazonBedrock = await import("@aws-sdk/client-bedrock-runtime");
      return { amazonBedrock };
    } catch (_a) {
      throw new Error(
        "Please install the @aws-sdk/client-bedrock-runtime package to use the AmazonBedrockEmbeddingFunction, `npm install -S @aws-sdk/client-bedrock-runtime`",
      );
    }
  }
}
