"use client";

import React, { createContext, useState, useContext, ReactNode } from "react";
import { Chat } from "@/lib/types";
import { addChatRecord } from "@/lib/server-utils";

type AppContextType = {
  chats: Chat[];
  activeChat: string;
  setActiveChat: (chatId: string) => void;
  clearActiveChat: () => void;
  addChat: (firstMessage: string) => Promise<string | undefined>;
  error: string;
  setError: (message: string) => void;
};

const AppContext = createContext<AppContextType | undefined>(undefined);

export const AppContextProvider: React.FC<{
  chats: Chat[];
  children: ReactNode;
}> = ({ chats, children }) => {
  const [appChats, setAppChats] = useState<Chat[]>(chats);
  const [activeChat, setActiveChat] = useState<string>("");
  const [error, setError] = useState<string>("");

  const addChat = async (firstMessage: string) => {
    try {
      const chat = await addChatRecord(firstMessage);
      setAppChats((prev) => [chat, ...prev]);
      return chat.id;
    } catch (e) {
      let errorMessage: string;
      if (e instanceof Error) {
        errorMessage = e.message;
      } else {
        errorMessage = String(e);
      }
      setError(errorMessage);
      return undefined;
    }
  };

  const clearActiveChat = () => {
    setActiveChat("");
  };

  return (
    <AppContext.Provider
      value={{
        chats: appChats,
        activeChat,
        setActiveChat,
        clearActiveChat,
        addChat,
        error,
        setError,
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
