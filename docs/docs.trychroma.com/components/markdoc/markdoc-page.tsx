"use client";

import React from "react";
import { AppContextProvider } from "@/context/app-context";

const MarkdocPage: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <AppContextProvider>
      <div className="w-full max-w-full h-full overflow-y-scroll py-10 pb-10 px-14 pl-20 prose dark:prose-invert">
        <div>{children}</div>
      </div>
    </AppContextProvider>
  );
};

export default MarkdocPage;
