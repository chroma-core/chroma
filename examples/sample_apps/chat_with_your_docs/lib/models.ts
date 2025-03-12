export interface Chunk {
  id: string;
  content: string;
  type: "code" | "docs";
  summary: string;
}

export interface Message {
  id: string;
  timestamp: string;
  role: "user" | "assistant";
  content: string;
  chunks?: Chunk[];
  retrievalTime?: number;
}

export interface Chat {
  id: string;
  title: string;
  created: string;
}

export interface Records {
  ids: string[];
  documents?: string[];
  embeddings?: string[];
  metadatas?: Record<string, string | number | boolean>[];
}
