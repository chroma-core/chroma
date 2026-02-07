import React from "react";
import Header84 from "@/components/ui/header-84";
import CloseButton84 from "@/components/ui/close-button-84";
import Link from "next/link";
import { AppPage } from "@/lib/content";
import NavbarButton from "@/components/header/navbar-button";

const updatesPages: AppPage[] = [
  { id: "migration", name: "Migration" },
  { id: "troubleshooting", name: "Troubleshooting" },
];

const UpdatesNavbar: React.FC = () => {
  return (
    <div className="flex flex-col w-full mt-1 gap-4 pb-4 ">
      <Header84 title="CHROMA UPDATES">
        <Link href="/">
          <CloseButton84 />
        </Link>
      </Header84>
      <div className="flex items-center gap-4 px-3">
        {updatesPages.map((page) => (
          <NavbarButton page={page} key={page.id} />
        ))}
      </div>
    </div>
  );
};

export default UpdatesNavbar;
