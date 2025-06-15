"use client";

import React, {
  createContext,
  useState,
  useContext,
  ReactNode,
  useEffect,
  useCallback,
} from "react";
import { AppError, Chat, Message } from "@/lib/types";
import { getAssistantResponse, getChatTitle } from "@/lib/ai-utils";
import { v4 as uuidv4 } from "uuid";
import {
  addChatRecord,
  addTelemetry,
  getChatMessages,
} from "@/lib/server-utils";
import { retrieveChunks } from "@/lib/retrieval";
import { notFound, useParams } from "next/navigation";

type AppContextType = {
  chats: Chat[];
  activeChat: Chat | null;
  error: string;
  setError: (message: string) => void;
  typingTitle: string;
  messages: Message[];
  setMessages: (message: Message[]) => void;
  clearChat: () => void;
  loading: boolean;
  submitMessage: (message: string, chat: Chat) => Promise<void>;
  activeResponse: string;
};

const AppContext = createContext<AppContextType | undefined>(undefined);

export const AppContextProvider: React.FC<{
  chats: Chat[];
  children: ReactNode;
  serverError?: AppError;
}> = ({ chats, children, serverError }) => {
  const [appChats, setAppChats] = useState<Chat[]>(chats);
  const [activeChat, setActiveChat] = useState<Chat | null>(null);
  const [error, setError] = useState<string>(serverError?.message || "");
  const [typingTitle, setTypingTitle] = useState<string>("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [activeResponse, setActiveResponse] = useState<string>("");
  const [isTypingTitleComplete, setIsTypingTitleComplete] =
    useState<boolean>(true);
  const params = useParams<{ chatId: string }>();

  useEffect(() => {
    if (!params.chatId || (activeChat && params.chatId === activeChat.id)) {
      return;
    }
    clearChat();
    const chatId = params.chatId as string;
    const chat = appChats.find((chat) => chat.id === params.chatId);
    if (!chat) {
      notFound();
    }
    getChatMessages(chatId).then((messagesResult) => {
      if (!messagesResult.ok) {
        setError(messagesResult.error.message);
      } else {
        setMessages(messagesResult.value);
      }
    });
  }, [activeChat, appChats, params]);

  const typeTitle = useCallback((fullTitle: string): Promise<void> => {
    return new Promise((resolve) => {
      setIsTypingTitleComplete(false);
      setTypingTitle("");

      let i = 0;
      const typeCharacter = () => {
        if (i <= fullTitle.length) {
          setTypingTitle(fullTitle.slice(0, i));
          i++;
          setTimeout(typeCharacter, 40);
        } else {
          setIsTypingTitleComplete(true);
          resolve();
        }
      };

      typeCharacter();
    });
  }, []);

  const updateActiveChat = useCallback(
    async (message: string) => {
      if (!activeChat) return;

      const titleResult = await getChatTitle(message);
      if (!titleResult.ok) {
        setError(titleResult.error.message);
        return;
      }

      const fullTitle = titleResult.value;

      await typeTitle(fullTitle);

      const updatedChat: Chat = { ...activeChat, title: fullTitle };
      setActiveChat(updatedChat);

      setAppChats((prev) => [
        ...prev.map((c) => (c.id === updatedChat.id ? updatedChat : c)),
      ]);

      const newChatResult = await addChatRecord(updatedChat);
      if (!newChatResult.ok) {
        setError(newChatResult.error.message);
        return;
      }
    },
    [activeChat, typeTitle, setError],
  );

  useEffect(() => {
    if (
      activeChat &&
      !activeChat.title &&
      messages.length > 0 &&
      isTypingTitleComplete
    ) {
      updateActiveChat(messages[0].content).finally();
    }
  }, [activeChat, messages, updateActiveChat, isTypingTitleComplete]);

  const submitMessage = async (content: string, chat: Chat) => {
    setLoading(true);

    if (!activeChat) {
      setAppChats((prev) => [chat, ...prev]);
      setActiveChat(chat);
    }

    const userMessage: Message = {
      id: uuidv4(),
      chat_id: chat.id,
      timestamp: new Date().toISOString(),
      role: "user",
      content,
    };

    let chatHistory: Message[] = [];
    setMessages((prev) => {
      chatHistory = [...prev];
      return [...prev, userMessage];
    });

    const userMessageResult = await addTelemetry({
      role: "user",
      content,
      chatId: chat.id,
    });
    if (!userMessageResult.ok) {
      setError(userMessageResult.error.message);
      return;
    }

    const chunksResult = await retrieveChunks(userMessage.content);
    if (!chunksResult.ok) {
      setError(chunksResult.error.message);
      return;
    }

    const retrievedChunks = chunksResult.value;

    const assistantStreamResult = await getAssistantResponse(
      userMessageResult.value,
      retrievedChunks,
      chatHistory,
    );
    if (!assistantStreamResult.ok) {
      setError(assistantStreamResult.error.message);
      return;
    }

    let response = "";
    for await (const textPart of assistantStreamResult.value) {
      response += textPart;
      setActiveResponse(response);
    }

    const assistantMessage: Message = {
      id: uuidv4(),
      chat_id: chat.id,
      timestamp: new Date().toISOString(),
      role: "assistant",
      content: response,
      chunks: [...retrievedChunks],
    };

    const assistantMessageResult = await addTelemetry({
      role: "assistant",
      content: response,
      chatId: chat.id,
      chunks: retrievedChunks,
    });
    if (!assistantMessageResult.ok) {
      setError(assistantMessageResult.error.message);
      return;
    }

    setActiveResponse("");
    setMessages((prev) => [...prev, assistantMessage]);
    setLoading(false);
  };

  const clearChat = () => {
    setMessages([]);
    setActiveChat(null);
    setActiveResponse("");
    setTypingTitle("");
    setIsTypingTitleComplete(true);
  };

  return (
    <AppContext.Provider
      value={{
        chats: appChats,
        activeChat,
        error,
        setError,
        typingTitle,
        messages,
        setMessages,
        clearChat,
        loading,
        submitMessage,
        activeResponse,
      }}
    >
      {children}
    </AppContext.Provider>
  );
};

export function useAppContext() {
  const context = useContext(AppContext);
  if (context === undefined) {
    throw new Error("useAppContext must be used within an AppProvider");
  }
  return context;
}
