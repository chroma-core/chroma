export type Message = {
  id: string;
  timestamp: string;
  role: "user" | "assistant";
  content: string;
};
