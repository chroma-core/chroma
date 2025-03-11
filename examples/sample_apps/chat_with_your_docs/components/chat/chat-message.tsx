import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import "highlight.js/styles/atom-one-dark.css";
import { Message } from "@/lib/models";
import Citation from "@/components/chat/citation";

const ChatMessage: React.FC<{ message: Message }> = ({ message }) => {
  const userStyle = "border border-black rounded-sm self-end";

  return (
    <div
      className={`p-2 px-5 max-w-[80%] prose ${message.role === "user" && userStyle}`}
    >
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeHighlight]}
      >
        {message.content}
      </ReactMarkdown>
      {message.role === "assistant" && (
        <div className="flex items-center gap-4">
          {message.chunks?.map((c) => <Citation key={c.id} chunk={c} />)}
        </div>
      )}
    </div>
  );
};

export default ChatMessage;
