"use client";

import React, {
  createContext,
  useContext,
  useState,
  ReactNode,
  useEffect,
  useCallback,
} from "react";
import { Chunk, Message } from "@/lib/types";
import { useAppContext } from "@/context/app-context";
import { retrieveChunks } from "@/lib/retrieval";
import { getAssistantResponse } from "@/lib/ai-utils";
import { addTelemetry, getChatMessages } from "@/lib/server-utils";

type ChatContextType = {
  messages: Message[];
  chunks: Chunk[];
  loading: boolean;
  activeResponse: string;
  addUserMessage: (message: string) => void;
};

const ChatContext = createContext<ChatContextType | undefined>(undefined);

export const ChatProvider: React.FC<{
  children: ReactNode;
  chatId: string;
}> = ({ children, chatId }) => {
  const { activeChat, setActiveChat, setError } = useAppContext();
  const [chatMessages, setChatMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(false);
  const [activeResponse, setActiveResponse] = useState<string>("");
  const [chunks, setChunks] = useState<Chunk[]>([]);

  const addUserMessage = async (messageContent: string) => {
    const message = await addTelemetry({
      role: "user",
      content: messageContent,
      chatId,
    });
    setChatMessages((prev) => [...prev, message]);
    await getAssistantMessage(message);
  };

  const getAssistantMessage = useCallback(
    async (userMessage: Message) => {
      try {
        setLoading(true);

        const chunks = await retrieveChunks(userMessage.content);
        setChunks(chunks);

        const assistantResponse = await getAssistantResponse(
          userMessage,
          chunks,
        );
        let streamedResponse = "";
        for await (const part of assistantResponse) {
          streamedResponse += part;
          setActiveResponse((prev) => prev + part);
        }

        const message = await addTelemetry({
          role: "assistant",
          content: streamedResponse,
          chatId,
          chunks,
        });
        setActiveResponse("");
        setChunks([]);
        setChatMessages((prev) => {
          return [...prev, message];
        });
        return message;
      } catch {
        // handle Chroma errors
        setError("Failed to get assistant response");
      } finally {
        setLoading(false);
      }
    },
    [chatId, setError],
  );

  useEffect(() => {
    if (chatMessages.length === 0) {
      getChatMessages(chatId).then((ms) => {
        setChatMessages(ms);
      });
    }
  }, []);

  return (
    <ChatContext.Provider
      value={{
        messages: chatMessages,
        loading,
        activeResponse,
        chunks,
        addUserMessage,
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
