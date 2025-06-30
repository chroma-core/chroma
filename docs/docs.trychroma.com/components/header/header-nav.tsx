"use client";

import React from "react";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import MenuItem from "../sidebar/menu-item";
import { useParams } from "next/navigation";

const HeaderNav: React.FC = () => {
  const params = useParams();
  // get current path from url using nextjs router
  const currentSection = sidebarConfig.find(
    (section) =>
      params?.slug &&
      Array.isArray(params.slug) &&
      params.slug.join("").startsWith(section.id),
  );

  return (
    <div className="flex items-center flex-shrink-0 px-5 border-b-[1px] dark:border-gray-700 w-full overflow-x-auto">
      {sidebarConfig.map((section) => (
        <MenuItem
          key={section.id}
          section={section}
          active={currentSection?.id === section.id}
        />
      ))}
    </div>
  );
};

export default HeaderNav;
