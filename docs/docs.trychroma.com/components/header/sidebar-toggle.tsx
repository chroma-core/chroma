import React from "react";
import { Drawer, DrawerContent, DrawerTrigger } from "@/components/ui/drawer";
import Sidebar from "@/components/sidebar/sidebar";
import { SidebarIcon } from "lucide-react";

const SidebarToggle: React.FC<{ path: string[] }> = ({ path }) => {
  return (
    <Drawer direction="left">
      <DrawerTrigger>
        <div className="fixed md:hidden bg-white dark:bg-black p-3 rounded-xl shadow-lg z-50 bottom-4 left-4 border dark:border-gray-700">
          <SidebarIcon children="w-5 h-5" />
        </div>
      </DrawerTrigger>
      <DrawerContent className="h-full w-[270px] bg-red-300 bg-[url(/composite_noise.jpg)] dark:backdrop-invert">
        <Sidebar path={path} mobile />
      </DrawerContent>
    </Drawer>
  );
};

export default SidebarToggle;
