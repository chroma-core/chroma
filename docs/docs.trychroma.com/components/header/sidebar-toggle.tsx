import React from "react";
import { Drawer, DrawerContent, DrawerTrigger } from "@/components/ui/drawer";
import Sidebar from "@/components/sidebar/sidebar";
import { SidebarIcon } from "lucide-react";

const SidebarToggle: React.FC<{ path: string[] }> = ({ path }) => {
  return (
    <Drawer direction="left">
      <DrawerTrigger>
        <div className="absolute -top-7 -left-14 md:hidden">
          <SidebarIcon children="w-5 h-5" />
        </div>
      </DrawerTrigger>
      <DrawerContent className="h-full w-[270px]">
        <Sidebar path={path} mobile />
      </DrawerContent>
    </Drawer>
  );
};

export default SidebarToggle;
