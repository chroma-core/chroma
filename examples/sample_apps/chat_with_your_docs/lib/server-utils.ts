"use server";

import { v4 as uuidv4 } from "uuid";
import { ChromaClient } from "chromadb";

export const generateUUID = async () => {
  return {
    id: uuidv4(),
    timestamp: new Date().toISOString(),
  };
};

export const getChromaClient = () => {
  return new ChromaClient({
    path: `${process.env.CHROMA_HOST}:${process.env.CHROMA_PORT}`,
    auth: {
      provider: "token",
      credentials: process.env.CHROMA_API_KEY,
      tokenHeaderType: "X_CHROMA_TOKEN",
    },
    tenant: process.env.CHROMA_TENANT_ID,
    database: process.env.CHROMA_DATABASE,
  });
};
