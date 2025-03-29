"use client";

import React from "react";
import { Chat } from "@/lib/types";
import { useParams } from "next/navigation";
import Link from "next/link";

const ChatButton: React.FC<{ chat: Chat }> = ({ chat }) => {
  const { chatId } = useParams();
  return (
    <Link href={`/chat/${chat.id}`}>
      <div
        className={`p-2 pl-3 rounded-md border border-black ${chatId === chat.id && "ring-1 ring-black"}`}
      >
        <div className="overflow-hidden text-ellipsis whitespace-nowrap">
          {chat.title || "New Chat"}
        </div>
      </div>
    </Link>
  );
};

export default ChatButton;
