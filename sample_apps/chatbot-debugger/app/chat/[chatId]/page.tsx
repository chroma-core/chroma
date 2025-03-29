import React from "react";
import { ChatProvider } from "@/context/chat-context";
import ChatMessages from "@/components/chat/chat-messages";

const ChatPage: React.FC<{
  params: Promise<{ chatId: string }>;
}> = async ({ params }) => {
  const { chatId } = await params;

  return (
    <ChatProvider chatId={chatId}>
      <div className="p-3">
        <ChatMessages />
      </div>
    </ChatProvider>
  );
};

export default ChatPage;
