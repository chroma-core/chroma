"use client";

import React, { useEffect } from "react";
import { useAppContext } from "@/context/app-context";

const NewChatPage: React.FC = () => {
  const { clearActiveChat } = useAppContext();

  useEffect(() => {
    clearActiveChat();
  }, [clearActiveChat]);

  return (
    <div className="flex w-full h-full justify-center items-center font-mono font-medium text-xl">
      How can I help you today?
    </div>
  );
};

export default NewChatPage;
