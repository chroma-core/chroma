import React from "react";
import { XIcon } from "lucide-react";

const SysUIHeader: React.FC<{
  children: React.ReactNode;
  onClick?: () => void;
}> = ({ children, onClick }) => {
  return (
    <div className="relative bg-white w-full py-2 px-[3px] h-fit border-b-[1px] border-black">
      <div
        className={`absolute top-1 right-3 flex items-center justify-center w-7 h-7 border border-black bg-white ${onClick && "cursor-pointer"}`}
        onClick={onClick}
      >
        <XIcon className="h-5 w-5 text-black" />
      </div>
      <div className="flex flex-col w-full gap-0.5">
        {[...Array(7)].map((_, index) => (
          <div key={index} className="w-full h-[1px] bg-black" />
        ))}
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-1 bg-white select-none text-sm font-mono">
          {children}
        </div>
      </div>
    </div>
  );
};

export default SysUIHeader;
