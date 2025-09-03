import React from "react";
import { Cross2Icon } from "@radix-ui/react-icons";

const Header84: React.FC<{ title: string; children?: React.ReactNode }> = ({
  title,
  children,
}) => {
  return (
    <div className="relative py-2 px-[3px] h-fit border-b-[1px] border-black dark:border-gray-300 dark:bg-gray-950">
      <div className="flex flex-col gap-0.5">
        {[...Array(7)].map((_, index) => (
          <div
            key={index}
            className="w-full h-[1px] bg-black dark:bg-gray-300"
          />
        ))}
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-0.5 bg-white border-black dark:border-gray-300 dark:bg-gray-950 font-mono select-none">
          {title}
        </div>
        {children}
      </div>
    </div>
  );
};

export default Header84;
