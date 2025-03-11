"use client";

import React, { useState } from "react";
import { Input } from "@/components/ui/input";
import ShadowButton from "@/components/ui/shadow-button";
import { ArrowRight } from "lucide-react";
import { useChat } from "@/context/chat-context";
import { useParams } from "next/navigation";

const InputBox: React.FC = () => {
  const { loading } = useChat();
  const [inputValue, setInputValue] = useState("");
  const [isHovered, setIsHovered] = useState(false);
  const { addUserMessage } = useChat();
  const { chatId } = useParams();

  const handleSubmit = (content: string) => {
    if (loading) return;
    addUserMessage(content, chatId as string);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && inputValue.trim() !== "") {
      handleSubmit(inputValue);
      setInputValue("");
    }
  };

  return (
    <div
      className={`flex items-center justify-between gap-2 h-12 w-full border border-black py-1 px-2 transition ${
        isHovered ? "ring ring-black" : ""
      }`}
    >
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
