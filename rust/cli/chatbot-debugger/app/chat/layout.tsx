import React from "react";
import SysUIContainer from "@/components/ui/sysui-container";
import InputBox from "@/components/chat/input-box";

const ChatPageLayout: React.FC<{ children: React.ReactNode }> = async ({
  children,
}) => {
  return (
    <div className="flex flex-col items-center justify-between gap-2 w-full h-full pt-3 pb-5 px-4">
      <SysUIContainer className="w-full h-full" title="CHROMA CHAT">
        {children}
      </SysUIContainer>

      <div className="flex-shrink-0 w-full">
        <InputBox />
      </div>
    </div>
  );
};

export default ChatPageLayout;
