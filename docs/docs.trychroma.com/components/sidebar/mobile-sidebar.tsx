"use client";

import { ChevronDown } from "lucide-react";
import PageIndex from "@/components/sidebar/page-index";
import React from "react";
import { usePathname } from "next/navigation";
import { AppSection } from "@/lib/content";

export default function MobileSidebar({
  currentSection,
  currentId,
}: {
  currentSection: AppSection;
  currentId: string;
}) {
  const [menuOpen, setMenuOpen] = React.useState(true);
  const pathName = usePathname();
  // const active = pathName === path;

  // strip the path to be the last part of the path
  const path = pathName.split("/").filter((p) => p !== "");
  // get the last segment
  const lastSegment = path[path.length - 1];

  // get the active section from the path
  const curr = currentSection.subsections?.flatMap(
    (subsection: any) =>
      subsection.pages?.filter((page: any) => page.id === lastSegment) || [],
  )[0];

  return (
    <>
      <div
        onClick={() => setMenuOpen(!menuOpen)}
        className="md:hidden flex items-center justify-between p-4 border-b dark:border-gray-700"
      >
        {currentSection.name}
        {curr?.name ? `: ${curr.name}` : ""}
        <ChevronDown
          className={`text-gray-500 transition-all ${menuOpen ? "" : "rotate-180"}`}
        />
      </div>

      <div
        className={`md:hidden flex flex-col pt-3 gap-2 w-full ${menuOpen ? "hidden" : "block"} `}
      >
        {currentSection.subsections?.map((subsection, index) => (
          <div key={subsection.id} className="flex flex-col gap-2">
            <PageIndex
              key={subsection.id}
              index={index}
              name={subsection.name}
              path={`/${currentSection.id}/${subsection.id}`}
              pages={subsection.pages || []}
            />
          </div>
        ))}
      </div>
    </>
  );
}
