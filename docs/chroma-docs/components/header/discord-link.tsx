import React from "react";
import Link from "next/link";
import UIButton from "@/components/ui/ui-button";
import { DiscordLogoIcon } from "@radix-ui/react-icons";

const DiscordLink: React.FC = async () => {
  const response = await fetch(
    `https://discord.com/api/guilds/1073293645303795742/widget.json`,
    { next: { revalidate: 3600 } },
  );
  const onlineUsers = response.ok
    ? (await response.json()).presence_count
    : undefined;

  return (
    <Link
      href="https://discord.gg/MMeYNTmh3x"
      target="_blank"
      rel="noopener noreferrer"
    >
      <UIButton className="flex items-center gap-1 p-[0.35rem] text-xs">
        <DiscordLogoIcon className="h-4 w-4" />
        {onlineUsers ? `${onlineUsers} online` : undefined}
      </UIButton>
    </Link>
  );
};

export default DiscordLink;
