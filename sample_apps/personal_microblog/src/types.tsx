export type Role = "user" | "assistant";

export interface PostModel {
  id: string;
  role: Role;
  body: string;
  date: string;
  reply?: string;
}
