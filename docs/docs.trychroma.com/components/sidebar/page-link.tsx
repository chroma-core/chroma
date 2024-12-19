"use client";

import React from "react";
import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";

const PageLink: React.FC<{
  id: string;
  name: string;
  path: string;
  sectionPage: boolean;
}> = ({ id, name, path, sectionPage = true }) => {
  const pathName = usePathname();
  const searchParams = useSearchParams();
  const active = pathName === path;
  const lang = searchParams.get("lang");

  return (
    <div
      key={id}
      className={`${sectionPage ? "pl-7" : "pl-3"} py-0.5 border-l border-gray-300 hover:border-gray-900 dark:border-gray-500 dark:hover:border-gray-200 ${active && "border-gray-900 dark:border-white font-bold"}`}
    >
      <Link href={lang ? `${path}?lang=${lang}` : path}>
        <p className="text-sm">{name}</p>
      </Link>
    </div>
  );
};

export default PageLink;
