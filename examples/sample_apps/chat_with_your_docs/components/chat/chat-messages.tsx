import React, { useEffect, useRef } from "react";
import { useChat } from "@/context/chat-context";
import ChatMessage from "@/components/chat/chat-message";
import { Loader } from "lucide-react";

const ChatMessages: React.FC = () => {
  const { messages, loading } = useChat();
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const prevMessagesLengthRef = useRef(messages.length);

  useEffect(() => {
    const hasNewUserMessage =
      messages.length > prevMessagesLengthRef.current &&
      messages[messages.length - 1].role === "user";

    prevMessagesLengthRef.current = messages.length;

    if (hasNewUserMessage && messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [messages]);

  return (
    <div className="flex flex-col gap-4 w-full h-full overflow-y-auto">
      <div className="flex flex-col gap-4 px-5 pb-12">
        {messages.map((m) => (
          <ChatMessage key={m.id} message={m} />
        ))}
        {loading && (
          <div>
            <Loader className="ml-2 animate-spin w-5 h-5 text-black" />
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
    </div>
  );
};

export default ChatMessages;
