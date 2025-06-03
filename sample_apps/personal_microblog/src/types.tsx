import { StreamableValue } from "ai/rsc";

export type TweetStatus = "created" | "processing" | "done" | "error";

export type Role = "user" | "assistant";

export interface TweetModel {
  id: string;
  threadParentId?: string
  role: Role;
  body: string;
  date: number;
  aiReplyId?: string;
  status?: TweetStatus;
}

export interface PartialAssistantPost {
  id: string;
  threadParentId?: string
  role: Role;
  body: string;
  date: number;
  aiReplyId?: string;
  status?: TweetStatus;
  stream: StreamableValue<string, any>;
}
