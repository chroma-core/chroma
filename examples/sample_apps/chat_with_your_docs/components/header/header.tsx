import React from "react";
import ChromaLogo from "../../public/chroma-workmark-color-128.svg";
import Link from "next/link";
import DiscordLink from "@/components/header/discord-link";
import XLink from "@/components/header/x-link";
import GithubLink from "@/components/header/github-link";

const Header: React.FC = () => {
  return (
    <div className="flex justify-between items-center w-full px-5 py-3 border-b">
      <Link href="https://trychroma.com" target="_blank">
        <ChromaLogo />
      </Link>
      <div className="flex items-center gap-2">
        <DiscordLink />
        <XLink />
        <GithubLink />
      </div>
    </div>
  );
};

export default Header;
