import React from "react";
import NewChatButton from "@/components/sidebar/new-chat-button";
import ChatButton from "@/components/sidebar/chat-button";

const Sidebar: React.FC = () => {
  return (
    <div className="h-full w-80 p-2">
      <div className="w-full h-full border border-double border-gray-600 p-1">
        <div className="flex flex-col gap-5 w-full h-full border border-double border-gray-600 py-3">
          <div className="flex-shrink-0 px-5">
            <NewChatButton />
          </div>
          <div className="flex-grow h-0 overflow-auto px-5">
            <div className="flex flex-col gap-2 mb-10 text-sm font-mono">
              <ChatButton title="Chroma cloud pricing" />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;
