"use client";

import React from "react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { ChevronUpDownIcon } from "@heroicons/react/24/solid";
import { usePathname } from "next/navigation";
import Link from "next/link";
import OpenAILogo from "../../public/openai-logo.svg";
import { Clipboard, CopyIcon } from "lucide-react";

const prompt = (page: string) => {
  return `Read https://docs.trychroma.com/${page} so I can ask questions about it.`;
};

const AskAI: React.FC<{ content: string }> = ({ content }) => {
  const pathname = usePathname();

  const copyToClipboard = () => {
    navigator.clipboard.writeText(content).finally();
  };

  return (
    <div className="absolute top-0 right-0">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="outline"
            className="w-[120px] pl-4 pr-2 justify-between focus-visible:outline-none"
          >
            <p className="font-mono">Ask AI</p>
            <ChevronUpDownIcon className="w-5 h-5" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent className="w-[230px]" align="end">
          <DropdownMenuItem className="flex w-full">
            {/* <Link
              href={`https://chat.openai.com/?prompt=${encodeURIComponent(prompt(pathname))}`}
              target="_blank"
              rel="noopener noreferrer"
            >
              <div className="flex flex-col gap-1.5">
                <div className="flex items-center gap-1">
                  <OpenAILogo className="w-[22px] h-[22px] dark:invert" />
                  <p>Open in ChatGPT</p>
                </div>
                <p className="text-xs">Ask questions about this page</p>
              </div>
            </Link> */}
          </DropdownMenuItem>
          <DropdownMenuItem
            className="flex items-center gap-1.5 cursor-pointer"
            onClick={copyToClipboard}
          >
            <Clipboard className="w-4 h-4 ml-[2px]" />
            <p>Copy this page</p>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
};

export default AskAI;
