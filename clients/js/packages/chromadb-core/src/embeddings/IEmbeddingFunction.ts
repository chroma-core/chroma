export type EmbeddingFunctionSpace = "cosine" | "l2" | "ip";

export interface IEmbeddingFunction {
  generate(texts: string[]): Promise<number[][]>;
  name?: string;
  defaultSpace?(): EmbeddingFunctionSpace;
  supportedSpaces?(): EmbeddingFunctionSpace[];
  buildFromConfig?(config: Record<string, any>): IEmbeddingFunction;
  getConfig?(): Record<string, any>;
  validateConfigUpdate?(
    oldConfig: Record<string, any>,
    newConfig: Record<string, any>,
  ): void;
  validateConfig?(config: Record<string, any>): void;
}

export type EmbeddingFunctionConstructor = (new (
  ...args: any[]
) => IEmbeddingFunction) & {
  name?: string;
};
