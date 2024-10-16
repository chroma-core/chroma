import React from "react";
import { AppSection } from "@/lib/content";

const MenuItem: React.FC<{ section: AppSection; active: boolean }> = ({
  section,
  active,
}) => {
  const Icon = section.icon;

  return (
    <div
      className={`flex items-center gap-2 text-gray-700/80 cursor-pointer hover:text-gray-800 dark:text-gray-400/80 dark:hover:text-gray-300`}
    >
      <div
        className={`flex items-center justify-center p-1.5 rounded ${active && "bg-chroma-orange/40 text-chroma-orange/90"}`}
      >
        <Icon className="w-5 h-5" />
      </div>
      <p
        className={`font-semibold select-none ${active && "text-chroma-orange"}`}
      >
        {section.name}
      </p>
    </div>
  );
};

export default MenuItem;
