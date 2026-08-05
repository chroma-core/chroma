"use client";

import React from "react";
import NewChatButton from "@/components/sidebar/new-chat-button";
import ChatButton from "@/components/sidebar/chat-button";
import { useAppContext } from "@/context/app-context";

const Sidebar: React.FC = () => {
  const { chats } = useAppContext();

  return (
    <div className="flex-shrink-0 h-full w-80 p-2">
      <div className="w-full h-full border border-double border-gray-600 p-1">
        <div className="flex flex-col gap-2 w-full h-full border border-double border-gray-600 py-3">
          <div className="flex-shrink-0 px-5 w-full">
            <NewChatButton />
          </div>
          <div className="flex-grow h-0 overflow-auto px-5">
            <div className="flex flex-col gap-2 pt-5 mb-10 text-sm font-mono">
              {chats.map((chat) => (
                <ChatButton key={chat.id || "new-chat"} chat={chat} />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;
