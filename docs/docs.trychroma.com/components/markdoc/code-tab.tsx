"use client";

import React, { useContext } from "react";
import AppContext from "@/context/app-context";
import { Tabs } from "@/components/ui/tabs";

const CodeTabs: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { language } = useContext(AppContext);
  return (
    <Tabs defaultValue="python" value={language} className="flex flex-col mt-5">
      {children}
    </Tabs>
  );
};

export default CodeTabs;
