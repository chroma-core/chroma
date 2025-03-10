import { PlusIcon } from "@radix-ui/react-icons";
import React from "react";

const NewChatButton: React.FC = () => {
  return (
    <div className="relative flex items-center justify-between p-1.5 px-2.5 bg-white border border-black cursor-pointer">
      <div className="absolute w-full h-full bg-black top-1 -right-1 -z-10" />
      <p className="font-mono text-sm">New Chat</p>
      <PlusIcon className="w-4 h-4" />
    </div>
  );
};

export default NewChatButton;
