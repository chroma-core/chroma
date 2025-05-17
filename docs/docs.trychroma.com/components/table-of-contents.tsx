"use client";

import { TextIcon } from "lucide-react";

// @ts-expect-error - ignore
export function TableOfContents({ toc }) {
  const indentations = ["pl-0", "pl-0", "pl-3", "pl-5", "pl-7", "pl-9"];

  return (
    <div className="sticky top-0 w-[300px] h-screen py-5 mx-10 hidden xl:block">
      {/* Title Section */}
      <div className="flex flex-row items-center font-bold mb-2">
        <TextIcon className="h-5 w-5 pr-1" />
        &nbsp;On this page
      </div>

      {/* Navigation Links */}
      <nav className="max-h-[calc(100vh-100px)] overflow-y-auto pr-2 pb-20">
        {
          // @ts-expect-error - ignore
          toc.map((item, index) => {
            const style = `mt-1 ${indentations.at(item.level) || "pl-0"}`;
            return (
              <div key={`${item.id}-${index}`} className={style}>
                <a
                  href={`#${item.id}`}
                  className={`text-gray-700 dark:text-gray-200 text-sm font-normal no-underline transition-all hover:text-blue-500
                    `}
                >
                  {item.title}
                </a>
              </div>
            );
          })
        }
      </nav>
    </div>
  );
}
