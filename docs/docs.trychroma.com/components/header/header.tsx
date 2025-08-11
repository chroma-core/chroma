import React from "react";
import Logo from "@/components/header/logo";
import ThemeToggle from "@/components/header/theme-toggle";
import GithubLink from "@/components/header/github-link";
import XLink from "@/components/header/x-link";
import DiscordLink from "@/components/header/discord-link";
import Link from "next/link";
import SearchBox from "@/components/header/search-box";
import YouTubeLink from "@/components/header/youtube-link";

const Header: React.FC = () => {
  return (
    <div className="flex items-center justify-between flex-shrink-0 p-3 px-5 h-14 border-b-[1px] dark:border-gray-700 ">
      <div className="flex items-center gap-5">
        <Link href="/">
          <Logo />
        </Link>
        <SearchBox />
      </div>
      <div className="flex items-center justify-between gap-2">
        <DiscordLink />
        <GithubLink />
        <XLink />
        <YouTubeLink />
        <ThemeToggle />
      </div>
    </div>
  );
};

export default Header;
