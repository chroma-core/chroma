import React from "react";

const ChatContainer: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  return (
    <div className="flex flex-col w-full h-full border border-black">
      <div className="relative py-2 px-[3px] h-fit border-b-[1px] border-black dark:border-gray-300 dark:bg-gray-950">
        <div className="flex flex-col gap-0.5">
          {[...Array(7)].map((_, index) => (
            <div
              key={index}
              className="w-full h-[1px] bg-black dark:bg-gray-300"
            />
          ))}
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-1 bg-white dark:bg-gray-950 font-mono select-none">
            CHROMA CHAT
          </div>
          <div className="absolute top-1 right-4 w-7 h-7 bg-white border border-black" />
        </div>
      </div>
      <div className="flex-grow ">{children}</div>
    </div>
  );
};

export default ChatContainer;
