import React from "react";
import Link from "next/link";
import { AppSection } from "@/lib/content";

const MenuItem: React.FC<{ section: AppSection; active: boolean }> = ({
  section,
  active,
}) => {
  const Icon = section.icon!;

  return (
    <Link
      className={`border-b-4 h-12 px-3 pt-3 pr-5 text-gray-700/80 ${active ? "border-chroma-orange " : "border-transparent hover:border-gray-100 dark:hover:border-gray-300"} ${!section.disable && "hover:text-gray-800"} dark:text-gray-400/80 dark:hover:text-gray-300 text-nowrap`}
      href={
        section.disable
          ? ""
          : `/${section.id}/${section.default || (section.pages ? section.pages[0].id : "")}`
      }
    >
      <div
        className={` flex ${section.tag ? "items-start" : "items-center"} gap-2  cursor-pointer `}
      >
        <div
          className={`flex items-center justify-center p-[5px] rounded-lg ${active && " ring-chroma-orange bg-chroma-orange/10 text-chroma-orange"}`}
        >
          <Icon className="w-4 h-4" />
        </div>
        <div className="flex flex-col">
          <p
            className={`font-semibold select-none ${active && "text-chroma-orange"}`}
          >
            {section.name}
          </p>
          {section.tag && (
            <div className="w-fit h-fit text-xs px-2 py-0.5 bg-gray-800 rounded-md text-gray-200">
              {section.tag}
            </div>
          )}
        </div>
      </div>
    </Link>
  );
};

export default MenuItem;
