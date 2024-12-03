"use client";

import React from "react";
import { AppSection } from "@/markdoc/content/sidebar-config";
import Link from "next/link";
import {
  BlocksIcon,
  BookText,
  GraduationCap,
  LucideIcon,
  RocketIcon,
  SquareTerminalIcon,
  Wrench,
} from "lucide-react";
import { usePathname, useSearchParams } from "next/navigation";

const icons: Record<string, LucideIcon> = {
  docs: BookText,
  production: RocketIcon,
  integrations: BlocksIcon,
  cli: SquareTerminalIcon,
  reference: Wrench,
  learn: GraduationCap,
};

const MenuItem: React.FC<{ section: AppSection; active: boolean }> = ({
  section,
  active,
}) => {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const Icon = icons[section.id];
  const sectionPath = `/${section.id}/${section.default || (section.pages ? section.pages[0].id : "")}`;
  const lang = searchParams.get("lang");

  return (
    <Link
      href={
        section.comingSoon
          ? ""
          : lang
            ? `${sectionPath}?lang=${lang}`
            : sectionPath
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
        {section.comingSoon && (
          <div className="inline-flex text-xs px-2 py-0.5 bg-gray-800 rounded-md text-gray-200">
            Coming Soon
          </div>
        )}
      </div>
    </Link>
  );
};

export default MenuItem;
