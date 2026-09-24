"use client";

import { PlusIcon } from "@radix-ui/react-icons";
import React from "react";
import ShadowButton from "@/components/ui/shadow-button";
import { useAppContext } from "@/context/app-context";
import { useRouter } from "next/navigation";

const NewChatButton: React.FC = () => {
  const { clearChat, loading } = useAppContext();
  const router = useRouter();

  const onClick = async () => {
    if (loading) {
      return;
    }
    clearChat();
    router.push("/");
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
