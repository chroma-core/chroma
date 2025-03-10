import React from "react";

const ChatButton: React.FC<{ title: string }> = ({ title }) => {
  return (
    <div className="p-2 rounded-md border border-black">
      <div className="overflow-hidden text-ellipsis whitespace-nowrap">
        {title}
      </div>
    </div>
  );
};

export default ChatButton;
