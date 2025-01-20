"use client";

import React from "react";
import { AppPage } from "@/lib/content";
import Link from "next/link";
import { usePathname } from "next/navigation";

const NavbarButton: React.FC<{ page: AppPage }> = ({ page }) => {
  const pathname = usePathname();
  const path = `/updates/${page.id}`;
  const active = path === pathname;

  return (
    <Link href={path}>
      <div className="relative border-[1px] px-3 py-1 border-gray-900 bg-white dark:bg-black dark:border-gray-600 font-mono text-sm select-none">
        <div
          className={`absolute top-1 left-1 w-full h-full -z-10 ${active ? "bg-chroma-orange" : "bg-black"}`}
        />
        {page.name}
      </div>
    </Link>
  );
};

export default NavbarButton;
