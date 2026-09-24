import { ChromaClientParams } from "chromadb";

export type Result<T, E> = { ok: true; value: T } | { ok: false; error: E };

export class AppError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AppError";
  }
}

export type MetadataValue = string | number | boolean;

export type Role = "user" | "assistant";
export type ChunkType = "docs" | "code";

export type AppParams = {
  chromaClientParams: ChromaClientParams;
  openAIKey: string;
};

export interface AuthOptions {
  provider: string;
  credentials: string;
  tokenHeaderType?: "AUTHORIZATION" | "X_CHROMA_TOKEN";
}

export type ChromaGetResult = {
  ids: string[];
  documents?: (string | null)[];
  metadatas?: ({ [key: string]: string | number | boolean } | null)[];
};

export type SourceField =
  | { from: "ids" }
  | { from: "documents" }
  | { from: "metadatas"; key: string };

export type MappingConfig<T> = {
  [K in keyof T]: SourceField;
};

export interface Chunk {
  id: string;
  content: string;
  type: ChunkType;
  summary?: string;
}

export const chunkMappingConfig: MappingConfig<Chunk> = {
  id: { from: "ids" },
  content: { from: "documents" },
  type: { from: "metadatas", key: "type" },
};

export interface Message {
  id: string;
  chat_id: string;
  timestamp: string;
  role: Role;
  content: string;
  chunks?: Chunk[];
  chunkIds?: string;
}

export const messageMappingConfig: MappingConfig<Message> = {
  id: { from: "ids" },
  chat_id: { from: "metadatas", key: "chat_id" },
  timestamp: { from: "metadatas", key: "timestamp" },
  role: { from: "metadatas", key: "role" },
  content: { from: "documents" },
  chunkIds: { from: "metadatas", key: "chunks" },
};

export interface Chat {
  id: string;
  title?: string;
  created: string;
}

export const chatMappingConfig: MappingConfig<Chat> = {
  id: { from: "ids" },
  title: { from: "documents" },
  created: { from: "metadatas", key: "created" },
};

export type NewMessageRequest = {
  chatId?: string;
  content: string;
  role: Role;
  chunks?: Chunk[];
};

export type UpdateMessagesRequest = {
  messageIds: string[];
  chatId: string;
};
