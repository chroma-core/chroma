"use client";

import React from "react";
import { AppContextProvider } from "@/context/app-context";

const MarkdocPage: React.FC<{ children: React.ReactNode; initialLang: string }> = ({ children, initialLang }) => {
  return (
    <AppContextProvider initialLang={initialLang}>
      <div className="pb-40 px-5 lg:px-14 lg:pl-20 outline-none">
        {children}
      </div>
    </AppContextProvider>
  );
};

export default MarkdocPage;
