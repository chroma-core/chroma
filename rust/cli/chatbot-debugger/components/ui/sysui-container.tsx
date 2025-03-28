import React from "react";
import SysUIHeader from "@/components/ui/sysui-header";
import { cn } from "@/lib/utils";

const SysUIContainer: React.FC<{
  title: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}> = ({ title, children, className }) => {
  return (
    <div
      className={cn(
        "relative flex flex-col w-[520px] border border-black",
        className,
      )}
    >
      <SysUIHeader>{title}</SysUIHeader>
      <div className="flex flex-col w-full h-full">{children}</div>
    </div>
  );
};

export default SysUIContainer;
