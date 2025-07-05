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
import ClaudeLogo from "../../public/claude-logo.svg";
import { Clipboard } from "lucide-react";

const prompt = (page: string) => {
  const path = `https://docs.trychroma.com/llms${page}.txt`;
  return `Read this webpage ${path}, so I can ask questions about it.`;
};

const AskAI: React.FC<{ content: string }> = ({ content }) => {
  const pathname = usePathname();

  const copyToClipboard = () => {
    navigator.clipboard.writeText(content).finally();
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="outline"
          className="flex gap-4 pl-4 pr-2 items-center justify-between focus-visible:outline-none"
        >
          <p className="font-mono">Ask this Page</p>
          <ChevronUpDownIcon className="w-5 h-5" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent className="w-[230px]" align="end">
        <DropdownMenuItem className="flex w-full">
          <Link
            href={`https://chat.openai.com/?prompt=${encodeURIComponent(prompt(pathname))}`}
            target="_blank"
            rel="noopener noreferrer"
          >
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center gap-1">
                <OpenAILogo className="w-[22px] h-[22px] dark:invert" />
                <p>Open in ChatGPT</p>
              </div>
            </div>
          </Link>
        </DropdownMenuItem>
        <DropdownMenuItem className="flex w-full">
          <Link
            href={`https://claude.ai/new/?q=${encodeURIComponent(prompt(pathname))}`}
            target="_blank"
            rel="noopener noreferrer"
          >
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center gap-1">
                <div className="flex items-center justify-center w-[22px]">
                  <ClaudeLogo className="w-[18px] h-[18px] dark:invert" />
                </div>
                <p>Open in Claude</p>
              </div>
            </div>
          </Link>
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
  );
};

export default AskAI;
