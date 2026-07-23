import React from "react";
import UIButton from "@/components/ui/ui-button";
import { GitHubLogoIcon } from "@radix-ui/react-icons";
import { formatToK } from "@/lib/utils";
import Link from "next/link";

const GithubLink: React.FC = async () => {
  let stars;
  try {
    const response = await fetch(
      `https://api.github.com/repos/chroma-core/chroma`,
    );
    stars = response.ok ? (await response.json()).stargazers_count : undefined;
    stars = formatToK(stars);
  } catch {
    stars = "Offline";
  }

  return (
    <Link
      href="https://github.com/chroma-core/chroma"
      target="_blank"
      rel="noopener noreferrer"
    >
      <UIButton className="flex items-center gap-2 p-[0.35rem] text-xs">
        <GitHubLogoIcon className="h-4 w-4" />
        {stars}
      </UIButton>
    </Link>
  );
};

export default GithubLink;
