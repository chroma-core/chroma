import React from "react";
import Sidebar from "@/components/sidebar/sidebar";
import { AppContextProvider } from "@/context/app-context";
import { Chat } from "@/lib/types";
import ErrorWindow from "@/components/ui/error-window";
import { getChats } from "@/lib/server-utils";

const AppLayout: React.FC<{ children: React.ReactNode }> = async ({
  children,
}) => {
  let chats: Chat[] = [];

  try {
    chats = await getChats();
  } catch {
    return (
      <div className="absolute bottom-4 right-4">
        <ErrorWindow message="Failed to get chats. Please verify that you are able to query your 'chats' collection." />
      </div>
    );
  }

  return (
    <AppContextProvider chats={chats}>
      <div className="flex justify-between w-full h-full">
        <Sidebar />
        <div className="flex-grow">{children}</div>
      </div>
    </AppContextProvider>
  );
};

export default AppLayout;
