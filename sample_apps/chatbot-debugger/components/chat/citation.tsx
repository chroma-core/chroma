import React from "react";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Code, File } from "lucide-react";
import { Chunk } from "@/lib/types";

const Citation: React.FC<{ chunk: Chunk }> = ({ chunk }) => {
  return (
    <HoverCard openDelay={0} closeDelay={0}>
      <HoverCardTrigger asChild>
        <div className="flex items-center justify-center p-2 border border-black rounded-sm cursor-pointer">
          {chunk.type === "code" && <Code className="w-4 h-4" />}
          {chunk.type === "docs" && <File className="w-4 h-4" />}
        </div>
      </HoverCardTrigger>
      <HoverCardContent className="text-xs">{chunk.summary}</HoverCardContent>
    </HoverCard>
  );
};

export default Citation;
