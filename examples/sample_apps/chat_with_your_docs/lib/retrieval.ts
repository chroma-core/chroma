"use server";

import OpenAI from "openai";
import axios from "axios";
import { Chunk, Message } from "@/lib/models";

export const embed = async (texts: string[]): Promise<number[][]> => {
  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const response = await openai.embeddings.create({
    model: "text-embedding-3-large",
    input: texts,
  });

  return response.data.map((item) => item.embedding);
};

export const getChromaCollection = async (
  collectionName: string,
): Promise<{ id: string }> => {
  const url = `https://api.trychroma.com:8000/api/v2/tenants/${process.env.CHROMA_TENANT_ID}/databases/${process.env.CHROMA_DATABASE}/collections/${collectionName}`;

  const headers = {
    "X-Chroma-Token": process.env.CHROMA_API_KEY,
  };

  try {
    const response = await axios.get(url, { headers });
    return response.data;
  } catch (error) {
    console.error("Error getting Chroma collection:", error);
    throw error;
  }
};

export const collectionAdd = async (
  collectionId: string,
  ids: string[],
  documents?: string[],
  metadatas?: {
    chat_id: string;
    role: "user" | "assistant";
    timestamp: string;
    chunks: string | undefined;
  }[],
) => {
  const url = `https://api.trychroma.com:8000/api/v2/tenants/${process.env.CHROMA_TENANT_ID}/databases/${process.env.CHROMA_DATABASE}/collections/${collectionId}/add`;

  const embeddings = documents ? await embed(documents) : undefined;

  const payload = {
    ids,
    documents,
    embeddings,
    metadatas,
  };

  const headers = {
    "Content-Type": "application/json",
    "X-Chroma-Token": process.env.CHROMA_API_KEY,
  };

  try {
    const response = await axios.post(url, payload, { headers });
    return response.data;
  } catch (error) {
    console.error("Error adding documents to Chroma collection:", error);
    throw error;
  }
};

const collectionQuery = async (
  collectionId: string,
  query: string,
): Promise<{
  chunks: {
    ids: string[][];
    documents: string[][];
    metadatas: { type: string }[][];
  };
  embedTime: number;
}> => {
  const url = `https://api.trychroma.com:8000/api/v2/tenants/${process.env.CHROMA_TENANT_ID}/databases/${process.env.CHROMA_DATABASE}/collections/${collectionId}/query`;
  const start = Date.now();
  const payload = {
    query_embeddings: await embed([query]),
    n_results: 5,
  };
  const elapsed = Date.now() - start;

  const headers = {
    "Content-Type": "application/json",
    "X-Chroma-Token": process.env.CHROMA_API_KEY,
  };

  try {
    const response = await axios.post(url, payload, { headers });
    return { chunks: response.data, embedTime: elapsed };
  } catch (error) {
    console.error("Error querying Chroma collection:", error);
    throw error;
  }
};

export const collectionGet = async (
  collectionId: string,
  ids?: string[],
  where?: any,
) => {
  const url = `https://api.trychroma.com:8000/api/v2/tenants/${process.env.CHROMA_TENANT_ID}/databases/${process.env.CHROMA_DATABASE}/collections/${collectionId}/get`;

  const payload = {
    where: where,
  };

  const headers = {
    "Content-Type": "application/json",
    "X-Chroma-Token": process.env.CHROMA_API_KEY,
  };

  try {
    const response = await axios.post(url, payload, { headers });
    return response.data;
  } catch (error) {
    console.error("Error getting from a Chroma collection:", error);
    throw error;
  }
};

export const addTelemetry = async (message: Message, chatId: string) => {
  const metadata = {
    chat_id: chatId,
    role: message.role,
    timestamp: message.timestamp,
    chunks: message.chunks?.map((c) => c.id).toString(),
  };

  const telemetryCollection = (await getChromaCollection("telemetry")) as {
    id: string;
  };

  await collectionAdd(
    telemetryCollection.id,
    [message.id],
    [message.content],
    [metadata],
  );
};

export const retrieveChunks = async (message: Message) => {
  const start = Date.now();
  const dataCollection = (await getChromaCollection("data")) as { id: string };
  const summariesCollection = (await getChromaCollection("data-summaries")) as {
    id: string;
  };

  const { chunks, embedTime } = await collectionQuery(
    dataCollection.id,
    message.content,
  );

  const time = Date.now() - (start + embedTime);

  const where = {
    $or: chunks.ids[0].map((chunkId) => {
      return { "chunk-id": chunkId };
    }),
  };

  const summaries = await collectionGet(
    summariesCollection.id,
    undefined,
    where,
  );

  const summaryByChunkId: Record<string, string> = {};
  summaries.documents.forEach((doc: string, i: number) => {
    summaryByChunkId[summaries.metadatas[i]["chunk-id"] as string] = doc;
  });

  return {
    time,
    chunks: chunks.ids[0].map((chunkId, i) => {
      return {
        id: chunkId,
        content: chunks.documents[0][i],
        type: chunks.metadatas[0][i]?.type,
        summary: summaryByChunkId[chunkId],
      } as Chunk;
    }),
  };
};
