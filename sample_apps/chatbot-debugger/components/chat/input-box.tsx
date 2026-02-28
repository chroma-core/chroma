"use client";

import React, { useState } from "react";
import { Input } from "@/components/ui/input";
import ShadowButton from "@/components/ui/shadow-button";
import { ArrowRight } from "lucide-react";
import { useAppContext } from "@/context/app-context";
import { createChat } from "@/lib/server-utils";

const InputBox: React.FC = () => {
  const { submitMessage, activeChat, loading } = useAppContext();
  const [inputValue, setInputValue] = useState("");
  const [isHovered, setIsHovered] = useState(false);

  const handleSubmit = async (content: string) => {
    const chat = activeChat || (await createChat());
    await submitMessage(content, chat);
  };

  const handleKeyDown = async (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && inputValue.trim() !== "" && !loading) {
      setInputValue("");
      await handleSubmit(inputValue);
    }
  };

  const handleClick = async () => {
    if (inputValue.trim() && !loading) {
      await handleSubmit(inputValue);
      setInputValue("");
    }
  };

  return (
    <div
      className={`relative flex items-center justify-between gap-2 h-12 w-full border border-black py-1 px-2 transition ${
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
      <ShadowButton onClick={handleClick}>
        <ArrowRight className="w-4 h-4" />
      </ShadowButton>
    </div>
  );
};

export default InputBox;
