"use client";

import React from "react";
import { useAppContext } from "@/context/app-context";
import ChatMessages from "@/components/chat/chat-messages";

const NewChatPage: React.FC = () => {
  const { messages } = useAppContext();
  return (
    <>
      {messages.length === 0 && (
        <div className="flex w-full h-full justify-center items-center font-mono font-medium text-xl">
          How can I help you today?
        </div>
      )}
      {messages.length > 0 && <ChatMessages />}
    </>
  );
};

export default NewChatPage;
