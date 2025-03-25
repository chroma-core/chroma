"use client";

import React, { useEffect, useState } from "react";
import UIButton from "@/components/ui/ui-button";
import { BellIcon } from "lucide-react";
import Link from "next/link";

const LAST_UPDATE = "01/15/2025";

const UpdatesLink: React.FC = () => {
  const [upToDate, setUpToDate] = useState<boolean>(true);

  useEffect(() => {
    const storedUpdate = localStorage.getItem("chromaUpdate");
    if (!storedUpdate || new Date(storedUpdate) < new Date(LAST_UPDATE)) {
      setUpToDate(false);
    }
  }, []);

  const handleUpdateClick = () => {
    setUpToDate(true);
    localStorage.setItem("chromaUpdate", LAST_UPDATE);
  };

  return (
    <Link href="/updates/migration" onClick={handleUpdateClick}>
      <UIButton
        className={`relative flex items-center justify-center p-[0.35rem] text-xs ${
          !upToDate &&
          "border-chroma-orange dark:border-chroma-orange border-[1px]"
        }`}
      >
        <BellIcon className={`h-4 w-4 ${!upToDate && "text-chroma-orange"}`} />&nbsp;Updates
      </UIButton>
    </Link>
  );
};

export default UpdatesLink;
