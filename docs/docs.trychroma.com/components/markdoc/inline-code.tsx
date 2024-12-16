import React from "react";

const InlineCode: React.FC<{ content: React.ReactNode }> = ({ content }) => {
  return (
    <span className="inline-flex items-center justify-center py-[1px] px-1.5 bg-slate-100 dark:bg-gray-800 rounded-md text-orange-600 text-sm font-medium font-mono border-[1px] border-gray-200 dark:border-gray-600">
      {content}
    </span>
  );
};

export default InlineCode;
