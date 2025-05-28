export type PostStatus = "processing" | "done" | "error";

export type Role = "user" | "assistant";

export interface PostModel {
  id: string;
  role: Role;
  body: string;
  date: string;
  replyId?: string;
  status: PostStatus;
}
