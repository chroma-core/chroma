import React from "react";
import GithubLink from "@/components/header/github-link";
import XLink from "@/components/header/x-link";
import DiscordLink from "@/components/header/discord-link";
import Link from "next/link";
import ChromaLogo from "../../public/chroma-workmark-color-128.svg";

const Header: React.FC = () => {
  return (
    <div className="flex items-center justify-between flex-shrink-0 p-3 px-5 h-12 border-b-[1px] dark:border-gray-700 ">
      <Link href="https://trychroma.com" target="_blank">
        <ChromaLogo className="w-24 h-8" />
      </Link>
      <div className="flex items-center justify-between gap-2">
        <DiscordLink />
        <GithubLink />
        <XLink />
      </div>
    </div>
  );
};

export default Header;
