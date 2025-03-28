"use client";

import React, { useEffect, useRef } from "react";
import ChatMessage from "@/components/chat/chat-message";
import { useAppContext } from "@/context/app-context";
import { LoaderCircle } from "lucide-react";

const ChatMessages: React.FC = () => {
  const { messages, activeResponse, loading } = useAppContext();

  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  const prevMessagesLengthRef = useRef(messages.length);
  const prevActiveResponseRef = useRef(activeResponse);

  useEffect(() => {
    if (messages.length > prevMessagesLengthRef.current) {
      const latestMessage = messages[messages.length - 1];
      if (latestMessage.role === "user") {
        scrollToBottom();
      }
    }
    prevMessagesLengthRef.current = messages.length;
  }, [messages]);

  useEffect(() => {
    const hadActiveResponse =
      prevActiveResponseRef.current !== null &&
      prevActiveResponseRef.current !== undefined &&
      prevActiveResponseRef.current !== "";
    const hasActiveResponse =
      activeResponse !== null &&
      activeResponse !== undefined &&
      activeResponse !== "";

    if (hasActiveResponse && !hadActiveResponse) {
      scrollToBottom();
    }

    prevActiveResponseRef.current = activeResponse;
  }, [activeResponse]);

  return (
    <div className="p-3 flex-grow h-0 w-full">
      <div className="flex gap-4 w-full h-full overflow-y-auto">
        <div className="flex-grow w-0 flex flex-col gap-4 px-5 pb-12 overflow-x-hidden">
          {messages.map((m) => (
            <ChatMessage key={m.id} message={m} />
          ))}
          {loading && (
            <div className="flex items-center gap-2 mt-2">
              <p className="text-lg font-medium">Reasoning</p>
              <LoaderCircle className="ml-2 animate-spin w-4 h-4 text-black" />
            </div>
          )}
          {activeResponse && (
            <ChatMessage
              active
              message={{
                id: "",
                timestamp: "",
                content: activeResponse,
                role: "assistant",
                chat_id: "",
              }}
            />
          )}
          <div ref={messagesEndRef} />
        </div>
      </div>
    </div>
  );
};

export default ChatMessages;
