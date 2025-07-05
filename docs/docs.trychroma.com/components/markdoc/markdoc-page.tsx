"use client";

import React, { Suspense } from "react";
import { AppContextProvider } from "@/context/app-context";

const MarkdocPage: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <Suspense>
      <AppContextProvider>
        <div className="pb-40 px-5 lg:px-14 lg:pl-20 outline-none">
          {children}
        </div>
      </AppContextProvider>
    </Suspense>
  );
};

export default MarkdocPage;
