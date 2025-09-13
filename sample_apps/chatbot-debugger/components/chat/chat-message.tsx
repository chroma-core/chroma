import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import "highlight.js/styles/atom-one-dark.css";
import Citation from "@/components/chat/citation";
import { Chunk, Message } from "@/lib/types";

const ChatMessage: React.FC<{ message: Message; active?: boolean }> = ({
  message,
  active,
}) => {
  return (
    <div
      className={`flex flex-col gap-4 max-w-[80%] ${message.role === "user" && "self-end"}`}
    >
      {message.role === "assistant" && !active && (
        <div className="flex items-center gap-2 mt-2">
          <p className="text-lg font-medium">Reasoning</p>
        </div>
      )}
      <div
        className={`p-2 px-5 prose ${message.role === "user" && "rounded-sm border border-black"}`}
      >
        <ReactMarkdown
          remarkPlugins={[remarkGfm]}
          rehypePlugins={[rehypeHighlight]}
        >
          {message.content}
        </ReactMarkdown>
      </div>
      {message.role === "assistant" && (
        <div className="flex gap-4">
          {message.chunks?.map((chunk: Chunk) => (
            <Citation chunk={chunk} key={chunk.id} />
          ))}
        </div>
      )}
    </div>
  );
};

export default ChatMessage;
