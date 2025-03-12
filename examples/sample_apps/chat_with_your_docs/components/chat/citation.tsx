import React from "react";
import { Chunk } from "@/lib/models";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Code, File } from "lucide-react";
import Link from "next/link";

const Citation: React.FC<{ chunk: Chunk }> = ({ chunk }) => {
  return (
    <HoverCard openDelay={0} closeDelay={0}>
      <Link
        href={`https://www.trychroma.com/${process.env.NEXT_PUBLIC_CHROMA_TEAM}/${process.env.NEXT_PUBLIC_CHROMA_DATABASE}/collections/data?record_id=${chunk.id}&embedding_model=openai-text-embedding-3-large`}
        target="_blank"
        rel="noopener noreferrer"
        className="block"
      >
        <HoverCardTrigger asChild>
          <div className="flex items-center justify-center p-2 border border-black rounded-sm cursor-pointer">
            {chunk.type === "code" && <Code className="w-4 h-4" />}
            {chunk.type === "docs" && <File className="w-4 h-4" />}
          </div>
        </HoverCardTrigger>
      </Link>
      <HoverCardContent className="text-xs">{chunk.summary}</HoverCardContent>
    </HoverCard>
  );
};

export default Citation;
