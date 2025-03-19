'use client'

import { TextIcon } from "lucide-react";
import { useEffect, useState } from "react";

// @ts-expect-error - ignore
export function TableOfContents({ toc }) {
  
    return (
      <div className="sticky top-0 w-[300px] h-full py-5 overflow-y-auto mx-10 hidden xl:block">
        {/* Title Section */}
        <div className="flex flex-row items-center font-bold">
          <TextIcon className="h-5 w-5 pr-1" />
          &nbsp;On this page
        </div>
  
        {/* Navigation Links */}
        <nav>
          {
          // @ts-expect-error - ignore
          toc.map((item) => (
            <div key={item.id} className={`mt-1 pl-${item.level * 1}`}>
              <a
                href={`#${item.id}`}
                className={`text-gray-700 dark:text-gray-200 text-sm font-normal no-underline transition-all hover:text-blue-500
                    `}
              >
                {item.title}
              </a>
            </div>
          ))}
        </nav>
      </div>
    );
  }