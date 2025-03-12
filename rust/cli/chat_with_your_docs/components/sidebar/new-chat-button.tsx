"use client";

import { PlusIcon } from "@radix-ui/react-icons";
import React from "react";
import ShadowButton from "@/components/ui/shadow-button";
import { useRouter } from "next/navigation";
import { createChat } from "@/lib/chats";

const NewChatButton: React.FC = () => {
  const router = useRouter();

  const onClick = async () => {
    try {
      const chat = await createChat();
      router.push(chat.id);
    } catch (error) {
      console.error("Navigation failed:", error);
    }
  };

  return (
    <ShadowButton onClick={onClick}>
      <div className="flex items-center justify-between w-full">
        <p className="font-mono text-sm">Start a new chat</p>
        <PlusIcon className="w-4 h-4" />
      </div>
    </ShadowButton>
  );
};

export default NewChatButton;
