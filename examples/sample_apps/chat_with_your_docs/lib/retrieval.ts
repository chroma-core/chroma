"use server";

import { getChromaClient } from "@/lib/server-utils";
import { Chunk, Message } from "@/lib/models";
import OpenAI from "openai";

export const addTelemetry = async (message: Message, chatId: string) => {
  const client = await getChromaClient();
  const telemetry_collection = await client.getOrCreateCollection({
    name: "telemetry",
  });

  const metadata = {
    chat_id: chatId,
    role: message.role,
    timestamp: message.timestamp,
  };

  if (message.chunks) {
    await telemetry_collection.add({
      ids: [message.id],
      documents: [message.content],
      metadatas: [
        {
          ...metadata,
          chunks: message.chunks.map((c) => c.id).toString(),
        },
      ],
    });
  } else {
    await telemetry_collection.add({
      ids: [message.id],
      documents: [message.content],
      metadatas: [metadata],
    });
  }
};

export const retrieveChunks = async (message: Message) => {
  const client = await getChromaClient();
  const data_collection = await client.getOrCreateCollection({
    name: "data-to-retrieve",
  });

  const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
  const response = await openai.embeddings.create({
    model: "text-embedding-3-large",
    input: message.content,
  });

  const chunks = await data_collection.query({
    queryEmbeddings: [response.data[0].embedding],
    nResults: 5,
  });

  console.log(chunks);

  return chunks.ids[0].map((chunkId, i) => {
    return {
      id: chunkId,
      content: chunks.documents[0][i],
      type: chunks.metadatas[0][i]?.type,
    } as Chunk;
  });
};
