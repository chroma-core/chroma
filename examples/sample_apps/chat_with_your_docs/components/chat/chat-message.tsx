import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import "highlight.js/styles/atom-one-dark.css";
import { Message } from "@/lib/models";

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
    </div>
  );
};

export default ChatMessage;
