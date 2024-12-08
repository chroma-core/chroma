import React from "react";
import { AppSection } from "@/markdoc/content/sidebar-config";
import Link from "next/link";

const MenuItem: React.FC<{ section: AppSection; active: boolean }> = ({
  section,
  active,
}) => {
  const Icon = section.icon!;

  return (
    <Link
      href={
        section.comingSoon
          ? "/docs/overview/introduction"
          : `/${section.id}/${section.default || (section.pages ? section.pages[0].id : "")}`
      }
    >
      <div
        className={`flex items-center gap-2 text-gray-700/80 cursor-pointer hover:text-gray-800 dark:text-gray-400/80 dark:hover:text-gray-300`}
      >
        <div
          className={`flex items-center justify-center p-1.5 rounded-lg ${active && "bg-chroma-orange/40 text-chroma-orange/90"}`}
        >
          <Icon className="w-5 h-5" />
        </div>
        <p
          className={`font-semibold select-none ${active && "text-chroma-orange"}`}
        >
          {section.name}
        </p>
      </div>
    </Link>
  );
};

export default MenuItem;
