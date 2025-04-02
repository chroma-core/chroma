import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import "highlight.js/styles/atom-one-dark.css";
import { Chunk, Message } from "@/lib/models";
import Citation from "@/components/chat/citation";
import Image from "next/image";
import Link from "next/link";

const ChatMessage: React.FC<{ message: Message; active?: boolean }> = ({
  message,
  active,
}) => {
  return (
    <div
      className={`flex flex-col gap-4 max-w-[80%] ${message.role === "user" && "self-end"}`}
    >
      {message.role === "assistant" && !active && (
        <div className="flex items-center text-lg font-medium">{`Retrieving (${message.retrievalTime?.toFixed(2)} ms)`}</div>
      )}
      {message.role === "assistant" && !active && (
        <div className="flex gap-4 ">
          {message.chunks?.map((chunk: Chunk) => (
            <Citation chunk={chunk} key={chunk.id} />
          ))}
        </div>
      )}
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
      <Link
        href={`https://www.trychroma.com/${process.env.NEXT_PUBLIC_CHROMA_TEAM}/${process.env.NEXT_PUBLIC_CHROMA_DATABASE}/collections/telemetry?record_id=${message.id}&embedding_model=openai-text-embedding-3-large`}
        target="_blank"
        rel="noopener noreferrer"
        className="block"
      >
        <div
          className={`flex items-center gap-1 -mt-3 text-sm  cursor-pointer ${message.role === "user" ? "justify-end mr-1" : "border-t border-gray-400 pt-0.5"} ${message.role === "assistant" && active && "hidden"}`}
        >
          <Image src="/logo-bw.svg" alt="Chroma Logo" width={20} height={13} />
          <p>Open in Chroma</p>
        </div>
      </Link>
    </div>
  );
};

export default ChatMessage;
