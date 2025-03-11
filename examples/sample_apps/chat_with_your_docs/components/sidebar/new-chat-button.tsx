import { PlusIcon } from "@radix-ui/react-icons";
import React from "react";
import ShadowButton from "@/components/ui/shadow-button";

const NewChatButton: React.FC = () => {
  return (
    <ShadowButton>
      <div className="flex items-center justify-between w-full">
        <p className="font-mono text-sm">New Chat</p>
        <PlusIcon className="w-4 h-4" />
      </div>
    </ShadowButton>
  );
};

export default NewChatButton;
