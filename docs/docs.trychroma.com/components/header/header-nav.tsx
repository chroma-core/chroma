'use client'

import React from "react";
import Logo from "@/components/header/logo";
import ThemeToggle from "@/components/header/theme-toggle";
import GithubLink from "@/components/header/github-link";
import XLink from "@/components/header/x-link";
import DiscordLink from "@/components/header/discord-link";
import Link from "next/link";
import SearchBox from "@/components/header/search-box";
import UpdatesLink from "@/components/header/updates-link";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import MenuItem from "../sidebar/menu-item";
import { useRouter } from "next/router";
import { useParams } from "next/navigation";

const HeaderNav: React.FC = () => {
  const params = useParams();
  // get current path from url using nextjs router
  const currentSection = sidebarConfig.find((section) =>
      params?.slug && Array.isArray(params.slug) && params.slug.join("").startsWith(section.id),
  );

  return (
    <div className="flex items-center flex-shrink-0 px-5 border-b-[1px] dark:border-gray-700 ">
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
