import React from "react";
import { Cross2Icon } from "@radix-ui/react-icons";

const CloseButton84: React.FC<{ onClick?: () => void }> = ({ onClick }) => {
  return (
    <div
      className="absolute right-4 top-[6px] px-1 bg-white dark:bg-gray-950 cursor-pointer"
      onClick={onClick || undefined}
    >
      <div className="flex items-center justify-center bg-white dark:bg-gray-950 border-[1px] border-black disabled:pointer-events-none focus-visible:outline-none">
        <Cross2Icon className="h-5 w-5" />
      </div>
    </div>
  );
};

export default CloseButton84;
