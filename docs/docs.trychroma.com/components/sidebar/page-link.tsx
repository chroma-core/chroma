"use client";

import React from "react";
import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { slugToPath } from "@/lib/content";

const PageLink: React.FC<{
  id: string;
  name: string;
  slug: string;
  sectionPage: boolean;
}> = ({ id, name, slug, sectionPage = true }) => {
  const pathName = usePathname();
  const searchParams = useSearchParams();
  const path = slugToPath(slug);
  const active = pathName === path;
  const lang = searchParams?.get("lang");

  return (
    <div
      key={id}
      className={`px-3 py-1 border-l-4 ${active && "border-blue-500 bg-blue-50 text-blue-500 font-bold hover:bg-blue-50 dark:bg-blue-900 dark:text-blue-100"} ${!active && "border-transparent text-gray-600 dark:text-gray-300 hover:bg-gray-50 hover:border-gray-200 dark:hover:bg-gray-800 dark:hover:border-gray-400"} `}
    >
      <Link href={lang ? `${path}?lang=${lang}` : path}>
        <p className="text-sm">{name}</p>
      </Link>
    </div>
  );
};

export default PageLink;
