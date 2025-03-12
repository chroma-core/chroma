import React, { createContext, useContext, useState, ReactNode } from "react";
import { Chunk, Message } from "@/lib/models";
import { getAssistantResponse } from "@/lib/ai-utils";
import { generateUUID } from "@/lib/utils";
import { addTelemetry, retrieveChunks } from "@/lib/retrieval";
import { v4 as uuidv4 } from "uuid";

type ChatContextType = {
  messages: Message[];
  chunks: Chunk[];
  activeResponse: string;
  addUserMessage: (content: string) => void;
  clearMessages: () => void;
  error: string | null;
  loading: boolean;
  retrievalTime: number | null;
  chatId: string;
};

const ChatContext = createContext<ChatContextType | undefined>(undefined);

export const ChatProvider = ({ children }: { children: ReactNode }) => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [chunks, setChunks] = useState<Chunk[]>([]);
  const [activeResponse, setActiveResponse] = useState<string>("");
  const [retrievalTime, setRetrievalTime] = useState<number | null>(null);
  const [chatId, setChatId] = useState<string>("");

  const addUserMessage = async (content: string) => {
    let currentChatId = chatId || uuidv4();
    if (!chatId) {
      setChatId(currentChatId);
    }
    let userMessage: Message;
    try {
      const { id, timestamp } = generateUUID();
      userMessage = { id, timestamp, role: "user", content };
      setMessages((prev) => [...prev, userMessage]);
    } catch (err) {
      setLoading(false);
      console.error("Failed to generate UUID:", err);
      setError("Failed to submit a new message.");
      return;
    }

    addTelemetry(userMessage, currentChatId).finally();

    setLoading(true);

    const { time, chunks } = await retrieveChunks(userMessage);
    setRetrievalTime(time / 1000);
    setChunks(chunks);

    try {
      const assistantResponse = await getAssistantResponse(userMessage, chunks);

      let streamedResponse = "";

      for await (const part of assistantResponse) {
        streamedResponse += part;
        setActiveResponse((prev) => prev + part);
      }

      const { id, timestamp } = generateUUID();

      const assistantMessage: Message = {
        id,
        timestamp,
        role: "assistant",
        content: streamedResponse,
        chunks,
        retrievalTime: time / 1000,
      };

      addTelemetry(assistantMessage, currentChatId).finally();
      setActiveResponse("");
      setRetrievalTime(null);
      setChunks([]);
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
      value={{
        messages,
        addUserMessage,
        clearMessages,
        error,
        loading,
        chunks,
        activeResponse,
        retrievalTime,
        chatId,
      }}
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
