import { expect, test } from '@jest/globals';
import chroma from '../initClient'
import { DOCUMENTS, IDS } from '../data';
import { IncludeEnum } from "../../src/types";
import { AmazonBedrockEmbeddingFunction } from "../../src/embeddings/AmazonBedrockEmbeddingFunction";

if (!process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
  test.skip("it should add Amazon Bedrock embeddings", async () => {
  });
} else {
  test("it should add Amazon Bedrock embeddings", async () => {
    await chroma.reset();
    const embedder = new AmazonBedrockEmbeddingFunction({ config: {
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        sessionToken: process.env.AWS_SESSION_TOKEN,
      },
      region: "us-east-1",
    }})
    const collection = await chroma.createCollection({ name: "test", embeddingFunction: embedder });
    const embeddings = await embedder.generate(DOCUMENTS);
    await collection.add({ ids: IDS, embeddings: embeddings });
    const count = await collection.count();
    expect(count).toBe(3);
    var res = await collection.get({
      ids: IDS, include: [
        IncludeEnum.Embeddings,
      ]
    });
    expect(res.embeddings).toEqual(embeddings); // reverse because of the order of the ids
  });
}
