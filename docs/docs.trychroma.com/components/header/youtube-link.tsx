import React from "react";
import Link from "next/link";
import UIButton from "@/components/ui/ui-button";
import YouTubeLogo from "../../public/youtube-logo.svg";

const YouTubeLink: React.FC = () => {
  return (
    <Link
      // hide by default on small screens
      className="hidden sm:block"
      href="https://www.youtube.com/@trychroma/featured"
      target="_blank"
      rel="noopener noreferrer"
    >
      <UIButton className="flex items-center gap-2 px-[0.4rem] py-[0.4rem] text-xs">
        <YouTubeLogo className="h-[14px] w-[14px] dark:invert" />
      </UIButton>
    </Link>
  );
};

export default YouTubeLink;
