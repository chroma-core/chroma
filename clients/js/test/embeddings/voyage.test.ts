import chroma from '../initClient'
import { DOCUMENTS, IDS } from '../data';
import { IncludeEnum } from "../../src/types";
import {VoyageAIEmbeddingFunction, InputType} from "../../src/embeddings/VoyageAIEmbeddingFunction";

if (!process.env.VOYAGE_API_KEY) {
  test.skip("it should add VoyageAI embeddings", async () => {
  });
} else {
  test("it should add VoyageAI embeddings", async () => {
    await chroma.reset();
    const embedder = new VoyageAIEmbeddingFunction({ voyageaiApiKey: process.env.VOYAGE_API_KEY || "", modelName: "voyage-2", batchSize: 2, inputType: InputType.DOCUMENT })
    const collection = await chroma.createCollection({ name: "test" ,embeddingFunction: embedder});
    const embeddings = await embedder.generate(DOCUMENTS);
    await collection.add({ ids: IDS, embeddings: embeddings });
    const count = await collection.count();
    expect(count).toBe(3);
    expect(embeddings.length).toBe(3);
    expect(embeddings[0].length).toBe(1024);
    expect(embeddings[1].length).toBe(1024);
    expect(embeddings[2].length).toBe(1024);
    var res = await collection.get({
      ids: IDS, include: [
        IncludeEnum.Embeddings,
      ]
    });
    expect(res.embeddings).toEqual(embeddings); // reverse because of the order of the ids
  });
}
