"use client";

import React, { useState } from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { CopyIcon } from "lucide-react";

const CopyButton: React.FC<{ content: string }> = ({ content }) => {
  const [copied, setCopied] = useState<boolean>(false);
  const [open, setOpen] = useState<boolean>(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(content).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <TooltipProvider delayDuration={350}>
      <Tooltip open={open}>
        <TooltipTrigger
          onMouseEnter={() => setOpen(true)}
          onMouseLeave={() => setOpen(false)}
        >
          <CopyIcon
            onClick={handleCopy}
            className="w-4 h-4 text-gray-400 hover:text-gray-200 cursor-pointer"
          />
        </TooltipTrigger>
        <TooltipContent
          side="top"
          sideOffset={10}
          className="flex items-center justify-center"
        >
          <p className="text-xs">{copied ? "Copied!" : "Copy"}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
};

export default CopyButton;
