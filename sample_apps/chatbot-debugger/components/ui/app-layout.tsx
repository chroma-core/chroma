import React from "react";
import Sidebar from "@/components/sidebar/sidebar";
import { AppContextProvider } from "@/context/app-context";
import { getChats } from "@/lib/server-utils";
import SysUIContainer from "@/components/ui/sysui-container";
import InputBox from "@/components/chat/input-box";
import AppErrorWindow from "@/components/ui/app-error-window";

const AppLayout: React.FC<{ children: React.ReactNode }> = async ({
  children,
}) => {
  const chatsResult = await getChats();

  return (
    <AppContextProvider
      chats={chatsResult.ok ? chatsResult.value : []}
      serverError={!chatsResult.ok ? chatsResult.error : undefined}
    >
      <div className="flex justify-between w-full h-full">
        <Sidebar />
        <div className="relative flex-grow">
          <AppErrorWindow />
          <div className="flex flex-col items-center justify-between gap-2 w-full h-full pt-3 pb-5 px-4">
            <div className="flex-grow w-full">
              <SysUIContainer className="w-full h-full" title="CHROMA CHAT">
                {children}
              </SysUIContainer>
            </div>
            <div className="flex-shrink-0 w-full">
              <InputBox />
            </div>
          </div>
        </div>
      </div>
    </AppContextProvider>
  );
};

export default AppLayout;
