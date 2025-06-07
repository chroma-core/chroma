import { StreamableValue } from "ai/rsc";

export type TweetStatus = "created" | "processing" | "done" | "error";

export type Role = "user" | "assistant";

export interface TweetModel {
  id: string;
  threadParentId?: string
  role: Role;
  body: string;
  citations: string[];
  date: number;
  aiReplyId?: string;
  status?: TweetStatus;
}

export interface PartialAssistantPost {
  // Must be a superset of TweetModel
  id: string;
  threadParentId?: string
  role: Role;
  body: string;
  citations: string[];
  date: number;
  aiReplyId?: string;
  status?: TweetStatus;

  bodyStream?: StreamableValue<string, any>;
  citationStream?: StreamableValue<string, any>;
}
