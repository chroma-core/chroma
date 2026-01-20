"use client";

import React, { Suspense } from "react";
import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { slugToPath } from "@/lib/content";

// Inner component that uses useSearchParams
const PageLinkInner: React.FC<{
  id: string;
  name: string;
  slug: string;
  path: string;
  active: boolean;
}> = ({ id, name, path, active }) => {
  const searchParams = useSearchParams();
  const lang = searchParams?.get("lang") || "python";

  return (
    <div
      key={id}
      className={`px-3 py-1 border-l-4 ${active && "border-blue-500 bg-blue-50 text-blue-500 font-bold hover:bg-blue-50 dark:bg-blue-900 dark:text-blue-100"} ${!active && "border-transparent text-gray-600 dark:text-gray-300 hover:bg-gray-50 hover:border-gray-200 dark:hover:bg-gray-800 dark:hover:border-gray-400"} `}
    >
      <Link href={lang !== "python" ? `${path}?lang=${lang}` : path}>
        <p className="text-sm">{name}</p>
      </Link>
    </div>
  );
};

// Fallback component for SSR - renders the same structure with default lang
const PageLinkFallback: React.FC<{
  id: string;
  name: string;
  path: string;
  active: boolean;
}> = ({ id, name, path, active }) => {
  return (
    <div
      key={id}
      className={`px-3 py-1 border-l-4 ${active && "border-blue-500 bg-blue-50 text-blue-500 font-bold hover:bg-blue-50 dark:bg-blue-900 dark:text-blue-100"} ${!active && "border-transparent text-gray-600 dark:text-gray-300 hover:bg-gray-50 hover:border-gray-200 dark:hover:bg-gray-800 dark:hover:border-gray-400"} `}
    >
      <Link href={path}>
        <p className="text-sm">{name}</p>
      </Link>
    </div>
  );
};

const PageLink: React.FC<{
  id: string;
  name: string;
  slug: string;
  sectionPage: boolean;
}> = ({ id, name, slug, sectionPage = true }) => {
  const pathName = usePathname();
  const path = slugToPath(slug);
  const active = pathName === path;

  return (
    <Suspense fallback={<PageLinkFallback id={id} name={name} path={path} active={active} />}>
      <PageLinkInner id={id} name={name} slug={slug} path={path} active={active} />
    </Suspense>
  );
};

export default PageLink;
