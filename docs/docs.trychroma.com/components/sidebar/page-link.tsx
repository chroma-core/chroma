"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { Badge } from "@/components/ui/badge";

const PageLink: React.FC<{
  id: string;
  name: string;
  path: string;
  sectionPage: boolean;
  latestUpdate?: string;
}> = ({ id, name, path, sectionPage = true, latestUpdate }) => {
  const [upToDate, setUpToDate] = useState<boolean>(true);
  const pathName = usePathname();
  const searchParams = useSearchParams();
  const active = pathName === path;
  const lang = searchParams.get("lang");

  useEffect(() => {
    if (!latestUpdate) {
      return;
    }
    const storedUpdate = localStorage.getItem(`chromaUpdate-${id}`);
    if (!storedUpdate || new Date(storedUpdate) < new Date(latestUpdate)) {
      setUpToDate(false);
    }
  }, []);

  const handleUpdateClick = () => {
    if (!latestUpdate) return;
    setUpToDate(true);
    localStorage.setItem(`chromaUpdate-${id}`, latestUpdate);
  };

  return (
    <div
      key={id}
      className={`${sectionPage ? "pl-7" : "pl-3"} py-0.5 border-l border-gray-300 hover:border-gray-900 dark:border-gray-500 dark:hover:border-gray-200 ${active && "border-gray-900 dark:border-white font-bold"}`}
    >
      <Link
        href={lang ? `${path}?lang=${lang}` : path}
        onClick={handleUpdateClick}
        className="flex items-center gap-2"
      >
        <p className="text-sm">{name}</p>
        {!upToDate && (
          <Badge className="text-[10px] font-normal py-[1px]">New</Badge>
        )}
      </Link>
    </div>
  );
};

export default PageLink;
