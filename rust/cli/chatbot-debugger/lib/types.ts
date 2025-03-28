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
  type: "code" | "docs";
  summary: string;
}

export const chunkMappingConfig: MappingConfig<Chunk> = {
  id: { from: "ids" },
  content: { from: "documents" },
  type: { from: "metadatas", key: "type" },
  summary: { from: "metadatas", key: "summary" },
};

export interface Message {
  id: string;
  timestamp: string;
  role: "user" | "assistant";
  content: string;
  chunks?: Chunk[];
  chunkIds?: string;
}

export const messageMappingConfig: MappingConfig<Message> = {
  id: { from: "ids" },
  timestamp: { from: "ids" },
  role: { from: "metadatas", key: "role" },
  content: { from: "documents" },
  chunkIds: { from: "metadatas", key: "chunks" },
};

export interface Chat {
  id: string;
  title: string;
  created: string;
}

export const chatMappingConfig: MappingConfig<Chat> = {
  id: { from: "ids" },
  title: { from: "documents" },
  created: { from: "metadatas", key: "created" },
};

export const messageValidator = (message: Message) => {
  return message.role === "user" || message.role === "assistant";
};

export const chunkValidator = (chunk: Chunk) => {
  return chunk.type === "code" || chunk.type === "docs";
};
