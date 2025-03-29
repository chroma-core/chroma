"use client";

import React, { useState } from "react";
import { Input } from "@/components/ui/input";
import ShadowButton from "@/components/ui/shadow-button";
import { ArrowRight } from "lucide-react";
import { useAppContext } from "@/context/app-context";
import { useRouter } from "next/navigation";

const InputBox: React.FC = () => {
  const { activeChat, addChat } = useAppContext();
  const [inputValue, setInputValue] = useState("");
  const [isHovered, setIsHovered] = useState(false);
  const router = useRouter();

  const handleSubmit = (content: string) => {
    if (!activeChat) {
      addChat(content).then((chatId) => {
        if (chatId) {
          router.push(`/chat/${chatId}`);
        }
      });
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && inputValue.trim() !== "") {
      setInputValue("");
      handleSubmit(inputValue);
    }
  };

  return (
    <div
      className={`relative flex items-center justify-between gap-2 h-12 w-full border border-black py-1 px-2 transition ${
        isHovered ? "ring ring-black" : ""
      }`}
    >
      {/*{chatId && (*/}
      {/*  <Link*/}
      {/*    href={`https://www.trychroma.com/${process.env.NEXT_PUBLIC_CHROMA_TEAM}/${process.env.NEXT_PUBLIC_CHROMA_DATABASE}/collections/telemetry?embedding_model=openai-text-embedding-3-large&where=%7B"%24and"%3A%5B%7B"chat-id"%3A%7B"%24eq"%3A"${chatId}"%7D%7D%5D%7D`}*/}
      {/*    target="_blank"*/}
      {/*    rel="noopener noreferrer"*/}
      {/*    className="block"*/}
      {/*  >*/}
      {/*    <div className="absolute -top-7 right-0 text-xs py-0.5 px-3 rounded-full border bg-white border-black cursor-pointer">*/}
      {/*      Debug Conversation*/}
      {/*    </div>*/}
      {/*  </Link>*/}
      {/*)}*/}
      <Input
        className="border-0 shadow-none rounded-none focus-visible:ring-0"
        value={inputValue}
        onChange={(e) => setInputValue(e.target.value)}
        onKeyDown={handleKeyDown}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
      />
      <ShadowButton
        onClick={() => {
          if (inputValue.trim()) {
            handleSubmit(inputValue);
            setInputValue("");
          }
        }}
      >
        <ArrowRight className="w-4 h-4" />
      </ShadowButton>
    </div>
  );
};

export default InputBox;
