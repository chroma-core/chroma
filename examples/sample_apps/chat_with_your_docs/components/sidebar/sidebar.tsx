import React from "react";
import { PlusIcon } from "@radix-ui/react-icons";

const Sidebar: React.FC = () => {
  return (
    <div className="h-full w-80 p-2">
      <div className="w-full h-full border border-double border-gray-600 p-1">
        <div className="w-full h-full border border-double border-gray-600 px-5 py-3">
          <div className="relative flex items-center justify-between p-1.5 px-2.5 bg-white border border-black cursor-pointer">
            <div className="absolute w-full h-full bg-black top-1 -right-1 -z-10" />
            <p className="font-mono text-sm">New Chat</p>
            <PlusIcon className="w-4 h-4" />
          </div>
        </div>
      </div>
    </div>
  );
};

export default Sidebar;
