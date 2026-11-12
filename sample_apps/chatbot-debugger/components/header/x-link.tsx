import React from "react";
import Link from "next/link";
import UIButton from "@/components/ui/ui-button";
import XLogo from "../../public/x-logo.svg";

const XLink: React.FC = () => {
  return (
    <Link
      href="https://x.com/trychroma"
      target="_blank"
      rel="noopener noreferrer"
    >
      <UIButton className="flex items-center gap-2 p-[0.35rem] text-xs">
        <XLogo className="h-[14px] w-[14px] invert dark:invert-0" />
        <p>19.6k</p>
      </UIButton>
    </Link>
  );
};

export default XLink;
