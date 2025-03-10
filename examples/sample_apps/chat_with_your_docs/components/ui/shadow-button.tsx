import React from "react";
import { cn } from "@/lib/utils";

const ShadowButton: React.FC<{
  children: React.ReactNode;
  className?: string;
}> = ({ children, className }) => {
  return (
    <div
      className={cn(
        "relative flex items-center justify-center p-1.5 px-2.5 bg-white border border-black cursor-pointer",
        className,
      )}
    >
      <div className="absolute w-full h-full bg-black top-1 -right-1 -z-10" />
      {children}
    </div>
  );
};

export default ShadowButton;
