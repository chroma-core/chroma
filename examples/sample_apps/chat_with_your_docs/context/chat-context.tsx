import React, { createContext, useContext, useState, ReactNode } from "react";
import { Message } from "@/lib/models";
import { generateUUID } from "@/lib/server-utils";
import { getAssistantResponse } from "@/lib/ai-utils"; // Import the server function

type ChatContextType = {
  messages: Message[];
  addUserMessage: (content: string) => void;
  clearMessages: () => void;
  error: string | null;
  loading: boolean;
};

const ChatContext = createContext<ChatContextType | undefined>(undefined);

export const ChatProvider = ({ children }: { children: ReactNode }) => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const addUserMessage = async (content: string) => {
    let userMessage: Message;
    try {
      const { id, timestamp } = await generateUUID();
      userMessage = { id, timestamp, role: "user", content };
      setMessages((prev) => [...prev, userMessage]);
    } catch (err) {
      setLoading(false);
      console.error("Failed to generate UUID:", err);
      setError("Failed to submit a new message.");
      return;
    }

    setLoading(true);

    try {
      const assistantResponse = await getAssistantResponse(
        userMessage,
        messages,
      );
      const { id, timestamp } = await generateUUID();
      const assistantMessage: Message = {
        id,
        timestamp,
        role: "assistant",
        content: assistantResponse,
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      console.error("Failed to generate assistant response:", err);
    }

    setLoading(false);
  };

  const clearMessages = () => {
    setMessages([]);
    setError(null);
  };

  return (
    <ChatContext.Provider
      value={{ messages, addUserMessage, clearMessages, error, loading }}
    >
      {children}
    </ChatContext.Provider>
  );
};

export const useChat = () => {
  const context = useContext(ChatContext);
  if (!context) {
    throw new Error("useChat must be used within a ChatProvider");
  }
  return context;
};
