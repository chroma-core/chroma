import { IEmbeddingFunction } from "./IEmbeddingFunction";

export class CloudflareWorkersAIEmbeddingFunction
  implements IEmbeddingFunction
{
  private apiUrl: string;
  private headers: { [key: string]: string };
  private maxBatchSize: number;

  constructor({
    apiToken,
    model,
    accountId,
    gatewayUrl,
    maxBatchSize,
    headers,
  }: {
    apiToken: string;
    model?: string;
    accountId?: string;
    gatewayUrl?: string;
    maxBatchSize?: number;
    headers?: { [key: string]: string };
  }) {
    model = model || "@cf/baai/bge-base-en-v1.5";
    this.maxBatchSize = maxBatchSize || 100;
    if (accountId === undefined && gatewayUrl === undefined) {
      throw new Error("Please provide either an accountId or a gatewayUrl.");
    }
    if (accountId !== undefined && gatewayUrl !== undefined) {
      throw new Error(
        "Please provide either an accountId or a gatewayUrl, not both.",
      );
    }
    if (gatewayUrl !== undefined && !gatewayUrl.endsWith("/")) {
      gatewayUrl += "/" + model;
    }
    this.apiUrl =
      gatewayUrl ||
      `https://api.cloudflare.com/client/v4/accounts/${accountId}/ai/run/${model}`;
    this.headers = headers || {};
    this.headers["Authorization"] = `Bearer ${apiToken}`;
  }

  public async generate(texts: string[]) {
    if (texts.length === 0) {
      return [];
    }
    if (texts.length > this.maxBatchSize) {
      throw new Error(
        `Batch too large ${texts.length} > ${this.maxBatchSize} (maximum batch size).`,
      );
    }
    try {
      const response = await fetch(this.apiUrl, {
        method: "POST",
        headers: this.headers,
        body: JSON.stringify({
          text: texts,
        }),
      });

      const data = (await response.json()) as {
        success?: boolean;
        messages: any[];
        errors?: any[];
        result: { shape: any[]; data: number[][] };
      };
      if (data.success === false) {
        throw new Error(`${JSON.stringify(data.errors)}`);
      }
      return data.result.data;
    } catch (error) {
      console.error(error);
      if (error instanceof Error) {
        throw new Error(`Error calling CF API: ${error}`);
      } else {
        throw new Error(`Error calling CF API: ${error}`);
      }
    }
  }
}
