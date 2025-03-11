"use client";

import ChatContainer from "@/components/chat/chat-container";
import InputBox from "@/components/chat/input-box";
import { ChatProvider } from "@/context/chat-context";
import ChatMessages from "@/components/chat/chat-messages";

export default function Home() {
  return (
    <div className="flex p-5 items-center justify-center w-full h-full">
      <ChatProvider>
        <ChatContainer>
          <div className="flex flex-col w-full h-full">
            <div className="flex-grow h-0 overflow-auto pt-4 px-3">
              <ChatMessages />
            </div>
            <div className="flex items-center justify-center p-5">
              <InputBox />
            </div>
          </div>
        </ChatContainer>
      </ChatProvider>
    </div>
  );
}
