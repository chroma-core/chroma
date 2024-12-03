"use client";

import React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";

const PageLink: React.FC<{ id: string; name: string; path: string }> = ({
  id,
  name,
  path,
}) => {
  const pathName = usePathname();
  const active = pathName === path;

  return (
    <div
      key={id}
      className={`pl-7 py-0.5 border-l border-gray-300 hover:border-gray-900 dark:border-gray-500 dark:hover:border-gray-200 ${active && "border-gray-900 dark:border-white font-bold"}`}
    >
      <Link href={path}>
        <p className="text-sm">{name}</p>
      </Link>
    </div>
  );
};

export default PageLink;
