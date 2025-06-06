import React from "react";
import { Drawer, DrawerContent, DrawerTrigger } from "@/components/ui/drawer";
import Sidebar from "@/components/sidebar/sidebar";
import { SidebarIcon } from "lucide-react";
import UIButton from "../ui/ui-button";

const SidebarToggle: React.FC<{ path: string[] }> = ({ path }) => {
  return (
    <Drawer direction="left">
      <DrawerTrigger asChild>
        <div className="fixed md:hidden z-50 bottom-4 left-4">
          <UIButton className="p-3">
            <SidebarIcon children="w-5 h-5" />
          </UIButton>
        </div>
      </DrawerTrigger>
      <DrawerContent className="h-full w-[270px] bg-[url(/composite_noise.jpg)] dark:bg-black dark:backdrop-invert">
        <Sidebar path={path} mobile />
      </DrawerContent>
    </Drawer>
  );
};

export default SidebarToggle;
