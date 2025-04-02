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

export const heartbeat = async () => {
  const url = "https://api.trychroma.com:8000/api/v2/heartbeat";
  try {
    const start = performance.now();
    await axios.get(url);
    console.log((performance.now() - start).toFixed(2));
    return performance.now() - start;
  } catch (error) {
    console.error("Error getting Chroma collection:", error);
    throw error;
  }
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
    "chat-id": string;
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
    metadatas: { type: string; summary: string }[][];
  };
  time: number;
}> => {
  const url = `https://api.trychroma.com:8000/api/v2/tenants/${process.env.CHROMA_TENANT_ID}/databases/${process.env.CHROMA_DATABASE}/collections/${collectionId}/query`;
  const payload = {
    query_embeddings: await embed([query]),
    n_results: 5,
  };
  const start = performance.now();

  const headers = {
    "Content-Type": "application/json",
    "X-Chroma-Token": process.env.CHROMA_API_KEY,
  };

  try {
    const response = await axios.post(url, payload, { headers });
    return { chunks: response.data, time: performance.now() - start };
  } catch (error) {
    console.error("Error querying Chroma collection:", error);
    throw error;
  }
};

export const addTelemetry = async (message: Message, chatId: string) => {
  const metadata = {
    "chat-id": chatId,
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
  const dataCollection = (await getChromaCollection("data")) as { id: string };

  const { chunks, time } = await collectionQuery(
    dataCollection.id,
    message.content,
  );

  return {
    time,
    chunks: chunks.ids[0].map((chunkId, i) => {
      return {
        id: chunkId,
        content: chunks.documents[0][i],
        type: chunks.metadatas[0][i]?.type,
        summary: chunks.metadatas[0][i]?.summary,
      } as Chunk;
    }),
  };
};
