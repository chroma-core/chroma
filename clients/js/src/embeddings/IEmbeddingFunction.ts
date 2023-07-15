export interface IEmbeddingFunction {
    generate(texts: string[]): Promise<number[][]>;
}
