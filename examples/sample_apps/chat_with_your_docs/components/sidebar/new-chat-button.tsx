import { PlusIcon } from "@radix-ui/react-icons";
import React from "react";
import ShadowButton from "@/components/ui/shadow-button";

const NewChatButton: React.FC = () => {
  return (
    <ShadowButton className="justify-between">
      <div className="absolute w-full h-full bg-black top-1 -right-1 -z-10" />
      <p className="font-mono text-sm">New Chat</p>
      <PlusIcon className="w-4 h-4" />
    </ShadowButton>
  );
};

export default NewChatButton;
