import React from "react";
import MenuItem from "@/components/sidebar/menu-item";
import layoutConfig, { getSectionDirectory } from "@/lib/content";
import { headers } from "next/headers";
import Outline from "@/components/sidebar/outline";

const Sidebar: React.FC = () => {
  const path = headers().get("x-pathname") || "";
  const currentSection = layoutConfig.find((section) =>
    path.startsWith(section.target),
  );

  if (!currentSection) {
    return null;
  }

  const sectionDirectory = getSectionDirectory(currentSection.id);

  return (
    <div className="flex flex-col h-full w-60 p-5 border-r-[1px]">
      <div className="flex flex-col gap-1 pb-10">
        {layoutConfig.map((section) => (
          <MenuItem
            key={section.id}
            section={section}
            active={section === currentSection}
          />
        ))}
      </div>
      <div>
        {currentSection.subSections.map((subSection) => (
          <Outline
            key={subSection}
            title={subSection}
            pages={sectionDirectory[subSection]}
            path={path}
          />
        ))}
      </div>
    </div>
  );
};

export default Sidebar;
