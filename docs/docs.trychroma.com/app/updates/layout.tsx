import React from "react";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import Link from "next/link";
import CloseButton84 from "@/components/ui/close-button-84";
import Header84 from "@/components/ui/header-84";
import { AppPage } from "@/lib/content";
import NavbarButton from "@/components/header/navbar-button";

const updatesPages: AppPage[] = [
  { id: "migration", name: "Migration" },
  { id: "troubleshooting", name: "Troubleshooting" },
];

const UpdatesLayout: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  return (
    <Dialog open={true}>
      <DialogContent
        className="flex flex-col overflow-hidden w-full mt-1 gap-4 pb-4 sm:rounded-none outline-none max-w-4xl xl:max-w-5xl h-[90%] border-black p-0"
      >
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
        <div className="flex-grow overflow-y-auto">{children}</div>
      </DialogContent>
    </Dialog>
  );
};

export default UpdatesLayout;
