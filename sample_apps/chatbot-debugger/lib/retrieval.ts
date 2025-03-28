"use server";

import OpenAI from "openai";
import { DATA_COLLECTION, getAppParams, recordsToObject } from "@/lib/utils";
import { getChromaClient, getChromaCollection } from "@/lib/server-utils";
import { Chunk, chunkMappingConfig, chunkValidator } from "@/lib/types";

export const embed = async (texts: string[]): Promise<number[][]> => {
  const openai = new OpenAI({ apiKey: getAppParams().openAIKey });
  try {
    const response = await openai.embeddings.create({
      model: "text-embedding-3-large",
      input: texts,
    });
    return response.data.map((item) => item.embedding);
  } catch {
    throw new Error("Fail to create embeddings");
  }
};

export const retrieveChunks = async (messageContent: string) => {
  const client = await getChromaClient();
  const dataCollection = await getChromaCollection(client, DATA_COLLECTION);

  const queryEmbedding = await embed([messageContent]);

  let result;
  try {
    result = await dataCollection.query({
      queryEmbeddings: queryEmbedding,
      nResults: 5,
    });
  } catch (e) {
    console.error(e);
    throw new Error("Failed to retrieve chunks");
  }

  return recordsToObject<Chunk>(
    {
      ids: result.ids[0],
      documents: result.documents[0],
      metadatas: result.metadatas[0],
    },
    chunkMappingConfig,
    "Some records in the data collection were corrupted. Please make sure they contain all the required fields for the app",
    chunkValidator,
  );
};
