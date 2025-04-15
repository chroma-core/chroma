import React from "react";
import { Playfair_Display } from "next/font/google";
import CopyButton from "@/components/markdoc/copy-button";
import { capitalize } from "@/lib/utils";

export const tabLabelStyle = `rounded-none px-4 py-2 text-xs font-medium tracking-wider border-b-[1px] border-transparent disabled:pointer-events-none disabled:opacity-50 data-[state=active]:bg-transparent data-[state=active]:text-chroma-orange data-[state=active]:shadow-none data-[state=active]:border-chroma-orange cursor-pointer select-none tracking-tight`;

const CodeBlockHeader: React.FC<{
  language: string;
  content: string;
}> = ({ language, content }) => {
  return (
    <div className="flex items-center justify-between bg-gray-900 rounded-t-sm">
      <div className={tabLabelStyle} data-state={"active"}>
        {capitalize(language)}
      </div>
      <div className="flex items-center pr-3">
        <CopyButton content={content} />
      </div>
    </div>
  );
};

export default CodeBlockHeader;
