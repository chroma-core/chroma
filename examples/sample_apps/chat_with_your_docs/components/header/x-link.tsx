import React from "react";
import Link from "next/link";
import XLogo from "../../public/x-logo.svg";
import HeaderButton from "@/components/header/header-button";

const XLink: React.FC = () => {
    return (
        <Link
            href="https://x.com/trychroma"
            target="_blank"
            rel="noopener noreferrer"
        >
            <HeaderButton className="flex items-center gap-2 p-[0.35rem] text-xs">
                <XLogo className="h-[14px] w-[14px] invert dark:invert-0" />
                <p>17.7k</p>
            </HeaderButton>
        </Link>
    );
};

export default XLink;
