import React, { useEffect, useRef } from "react";
import { useChat } from "@/context/chat-context";
import ChatMessage from "@/components/chat/chat-message";
import { LoaderCircle } from "lucide-react";
import { Chunk } from "@/lib/models";
import Citation from "@/components/chat/citation";

const ChatMessages: React.FC = () => {
  const { messages, loading, chunks, activeResponse, retrievalTime } =
    useChat();

  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  const prevMessagesLengthRef = useRef(messages.length);
  const prevChunksLengthRef = useRef(chunks.length);
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
    if (chunks.length > 0 && prevChunksLengthRef.current === 0) {
      scrollToBottom();
    }

    prevChunksLengthRef.current = chunks.length;
  }, [chunks]);

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
    <div className="flex flex-col gap-4 w-full h-full overflow-y-auto">
      {messages.length === 0 && (
        <div className="flex w-full h-full justify-center items-center font-mono font-medium text-xl">
          How can I help you today?
        </div>
      )}
      <div className="flex flex-col gap-4 px-5 pb-12">
        {messages.map((m) => (
          <ChatMessage key={m.id} message={m} />
        ))}
        {loading && retrievalTime === null && (
          <div className="flex items-center gap-2">
            <p className="text-lg font-medium">Retrieving</p>
            <LoaderCircle className="ml-2 animate-spin w-4 h-4 text-black" />
          </div>
        )}
        {retrievalTime !== null && (
          <div className="flex items-center text-lg font-medium">{`Retrieving (${retrievalTime} seconds)`}</div>
        )}
        <div className="flex gap-4">
          {chunks.map((chunk: Chunk) => (
            <Citation chunk={chunk} key={chunk.id} />
          ))}
        </div>
        {activeResponse && (
          <div className="flex items-center gap-2 mt-2">
            <p className="text-lg font-medium">Reasoning</p>
            {loading && (
              <LoaderCircle className="ml-2 animate-spin w-4 h-4 text-black" />
            )}
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
            }}
          />
        )}
        <div ref={messagesEndRef} />
      </div>
    </div>
  );
};

export default ChatMessages;
