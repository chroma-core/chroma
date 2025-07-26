"use client";

import React from "react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { usePathname } from "next/navigation";
import Link from "next/link";
import OpenAILogo from "../../public/openai-logo.svg";
import ClaudeLogo from "../../public/claude-logo.svg";
import { ChevronDown, Clipboard } from "lucide-react";
import UIButton from "@/components/ui/ui-button";

const prompt = (page: string, content?: string) => {
  const pagePath = page.replaceAll("/", "-");
  const path = content
    ? `https://docs.trychroma.com/llms${pagePath}.txt`
    : `https://docs.trychroma.com/${pagePath}`;
  return `Read this webpage ${path}, so I can ask questions about it.`;
};

const AskAI: React.FC<{ content?: string }> = ({ content }) => {
  const pathname = usePathname();

  const copyToClipboard = (content: string) => {
    navigator.clipboard.writeText(content).finally();
  };

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <UIButton
          variant="outline"
          className="flex gap-1 pl-3 pr-2 items-center justify-between focus-visible:outline-none"
        >
          <p>Ask this Page</p>
          <ChevronDown className="w-4 h-4" />
        </UIButton>
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
            href={`https://claude.ai/new/?q=${encodeURIComponent(prompt(pathname, content))}`}
            target="_blank"
            rel="noopener noreferrer"
          >
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center gap-1">
                <div className="flex items-center justify-center w-[22px]">
                  <ClaudeLogo className="w-[17px] h-[17px]" />
                </div>
                <p>Open in Claude</p>
              </div>
            </div>
          </Link>
        </DropdownMenuItem>
        {content && (
          <DropdownMenuItem
            className="flex items-center gap-1.5 cursor-pointer"
            onClick={() => copyToClipboard(content)}
          >
            <Clipboard className="w-4 h-4 ml-[2px]" />
            <p>Copy this page</p>
          </DropdownMenuItem>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
};

export default AskAI;
