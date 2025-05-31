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
