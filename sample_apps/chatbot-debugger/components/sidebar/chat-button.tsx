"use client";

import React from "react";
import { Chat } from "@/lib/types";
import Link from "next/link";
import { useAppContext } from "@/context/app-context";
import { useParams } from "next/navigation";

const ChatButton: React.FC<{ chat: Chat }> = ({ chat }) => {
  const { typingTitle, activeChat } = useAppContext();
  const params = useParams<{ chatId: string }>();

  const active =
    (activeChat !== null && activeChat.id === chat.id) ||
    params.chatId === chat.id;

  return (
    <Link href={`/${chat.id}`}>
      <div
        className={`p-2 pl-3 rounded-md border border-black ${active && "ring-1 ring-black"}`}
      >
        <div className="overflow-hidden text-ellipsis whitespace-nowrap">
          {(!active || !typingTitle) && (chat.title || "New Chat")}
          {active && typingTitle && typingTitle}
        </div>
      </div>
    </Link>
  );
};

export default ChatButton;
