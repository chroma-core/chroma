import React from "react";
import { Chunk } from "@/lib/models";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { Code, File } from "lucide-react";
import remarkGfm from "remark-gfm";
import rehypeHighlight from "rehype-highlight";
import ReactMarkdown from "react-markdown";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

const Citation: React.FC<{ chunk: Chunk }> = ({ chunk }) => {
  return (
    <div>
      <Dialog>
        <DialogTrigger>
          <div className="p-2 border border-black rounded-sm cursor-pointer">
            {chunk.type === "code" ? (
              <Code className="w-5 h-5" />
            ) : (
              <File className="w-5 h-5" />
            )}
          </div>
        </DialogTrigger>
        <DialogContent className="w-[90%] h-[80%] overflow-auto">
          <DialogHeader>
            <DialogTitle>{chunk.type}</DialogTitle>
            {chunk.content}
          </DialogHeader>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default Citation;
