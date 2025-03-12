"use client";

import React, { useState } from "react";
import { Copy } from "lucide-react";

const extractText = (node: React.ReactNode): string => {
  if (typeof node === "string" || typeof node === "number") {
    return node.toString();
  }
  if (Array.isArray(node)) {
    return node.map(extractText).join("");
  }
  if (React.isValidElement(node)) {
    return extractText(node.props.children);
  }
  return "";
};

const CliBlock: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [tooltipText, setTooltipText] = useState("Copy");
  const [open, setOpen] = useState(false);

  const handleCopy = () => {
    const textToCopy = extractText(children);
    navigator.clipboard
      .writeText(textToCopy)
      .then(() => {
        setTooltipText("Copied!");
        setTimeout(() => {
          setTooltipText("Copy");
        }, 2000);
      })
      .catch((err) => {
        console.error("Failed to copy text: ", err);
      });
  };

  return (
    <div className="flex items-center gap-4 font-mono text-sm rounded-md bg-gray-100 text-black w-fit px-4 py-2">
      {children}
      <button
        className="relative"
        onClick={handleCopy}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
      >
        {open && (
          <div className="absolute flex items-center justify-center -top-8 -right-5 font-sans text-xs py-1 w-14 rounded-lg bg-gray-900 text-white">
            {tooltipText}
          </div>
        )}
        <Copy className="w-4 h-4" />
      </button>
    </div>
  );
};

export default CliBlock;
