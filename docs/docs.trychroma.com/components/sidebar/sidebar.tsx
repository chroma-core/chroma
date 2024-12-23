import React from "react";
import MenuItem from "@/components/sidebar/menu-item";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import PageIndex from "@/components/sidebar/page-index";
import path from "path";
import fs from "fs";
import matter from "gray-matter";
import ScrollableContent from "./scrollable-content";

const generatePages = (slug: string[]): { id: string; name: string }[] => {
  const dirPath = path.join(process.cwd(), "markdoc", "content", ...slug);
  const files = fs.readdirSync(dirPath);

  const pages = [];

  for (const file of files) {
    if (file.endsWith(".md")) {
      const filePath = path.join(dirPath, file);
      const content = fs.readFileSync(filePath, "utf-8");

      const { data } = matter(content);
      if (data.id && data.name) {
        pages.push({ id: data.id, name: data.name });
      }
    }
  }

  return pages;
};

const Sidebar: React.FC<{ path: string[]; mobile?: boolean }> = ({
  path,
  mobile,
}) => {
  const currentSection = sidebarConfig.find((section) =>
    path.join("").startsWith(section.id),
  );

  if (!currentSection) {
    return null;
  }

  const allSectionPages: string[] = [
    ...(currentSection.pages || []).map((p) => p.id),
  ];
  currentSection.subsections?.forEach((subsection) => {
    allSectionPages.push(...(subsection.pages?.map((p) => p.id) || []));
  });

  return (
    <div
      className={`h-full xl:ml-[calc((100vw-1256px)/2)] ${!mobile && "hidden md:block"}`}
    >
      <div className="flex flex-col h-full w-64 p-5 border-r-[1px] flex-shrink-0 dark:border-gray-700">
        <div className="flex flex-col gap-y-1.5 pb-10">
          {sidebarConfig.map((section) => (
            <MenuItem
              key={section.id}
              section={section}
              active={currentSection.id === section.id}
            />
          ))}
        </div>
        <ScrollableContent pagesIndex={allSectionPages}>
          {currentSection.pages && (
            <div className="flex flex-col gap-2">
              <PageIndex
                path={`/${currentSection.id}`}
                pages={currentSection.pages}
              />
            </div>
          )}
          {currentSection.subsections?.map((subsection) => (
            <PageIndex
              key={subsection.id}
              name={subsection.name}
              path={`/${currentSection.id}/${subsection.id}`}
              pages={
                subsection.generatePages
                  ? generatePages([currentSection.id, subsection.id])
                  : subsection.pages || []
              }
            />
          ))}
        </ScrollableContent>
      </div>
    </div>
  );
};

export default Sidebar;
