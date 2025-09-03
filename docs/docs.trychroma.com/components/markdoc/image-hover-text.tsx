import React from "react";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import Image from "next/image";
import Header84 from "@/components/ui/header-84";

const ImageHoverText: React.FC<{ src: string; children: React.ReactNode }> = ({
  src,
  children,
}) => {
  return (
    <HoverCard>
      <HoverCardTrigger>
        <span>{children?.toString()}</span>
      </HoverCardTrigger>
      <HoverCardContent className="p-0 w-[340px] h-[254px] rounded-none border border-black grid grid-rows-[auto,1fr]">
        <Header84 title="Chroma Cloud" />
        <div className="flex items-center justify-center w-full h-full overflow-hidden bg-white">
          <Image
            src={`/${src}`}
            alt="Chroma hover image"
            priority
            width={350}
            height={200}
            className="object-contain h-auto w-auto max-w-full max-h-full"
          />
        </div>
      </HoverCardContent>
    </HoverCard>
  );
};

export default ImageHoverText;
