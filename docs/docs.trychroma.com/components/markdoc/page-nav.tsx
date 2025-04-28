"use client";

import React from "react";
import Link from "next/link";
import { ArrowLeft, ArrowRight } from "lucide-react";
import { slugToPath } from "@/lib/content";

const PageNav: React.FC<{
  slug: string;
  name: string;
  type: "prev" | "next";
}> = ({ slug, name, type }) => {
  return (
    <Link
      href={slugToPath(slug)}
      onClick={() => {
        sessionStorage.removeItem("sidebarScrollPosition");
      }}
    >
      <div className="flex items-center gap-2">
        {type === "prev" && (
          <>
            <ArrowLeft className="w-4 h-4" />
            <p>{name}</p>
          </>
        )}
        {type === "next" && (
          <>
            <p>{name}</p>
            <ArrowRight className="w-4 h-4" />
          </>
        )}
      </div>
    </Link>
  );
};

export default PageNav;
