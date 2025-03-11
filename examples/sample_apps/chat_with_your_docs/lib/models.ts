export interface Chunk {
  id: string;
  content: string;
  type: "code" | "documentation";
}

export interface Message {
  id: string;
  timestamp: string;
  role: "user" | "assistant";
  content: string;
  chunks?: Chunk[];
}

export interface Chat {
  id: string;
  title: string;
  created: string;
}
