import React from "react";

const InlineCode: React.FC<{ content: React.ReactNode }> = ({ content }) => {
  return (
    <span className="inline-flex items-center justify-center py-0.5 px-2 bg-slate-100 dark:bg-gray-700 rounded-md text-chroma-orange text-sm font-medium font-mono border-[1px] border-gray-200 dark:border-gray-600">
      {content}
    </span>
  );
};

export default InlineCode;
