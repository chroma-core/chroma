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
      <UIButton className="p-[0.35rem]">
        <XLogo className="h-4 w-4 invert dark:invert-0" />
      </UIButton>
    </Link>
  );
};

export default XLink;
