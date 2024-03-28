import { IEmbeddingFunction } from "./IEmbeddingFunction";

export class OllamaEmbeddingFunction implements IEmbeddingFunction {
    private readonly url: string;
    private readonly model: string;

    constructor({ url, model }: { url: string, model: string }) {
        // we used to construct the client here, but we need to async import the types
        // for the openai npm package, and the constructor can not be async
        this.url = url;
        this.model = model;
    }

    public async generate(texts: string[]) {
        let embeddings:number[][] = [];
        for (let text of texts) {
            const response = await fetch(this.url, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ 'model':this.model, 'prompt': text })
            });

            if (!response.ok) {
                throw new Error(`Failed to generate embeddings: ${response.status} (${response.statusText})`);
            }
            let finalResponse = await response.json();
            embeddings.push(finalResponse['embedding']);
        }
        return embeddings;
    }

}
