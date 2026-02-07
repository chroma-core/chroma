"use client";

import React, { Suspense } from "react";
import { AppContextProvider } from "@/context/app-context";

const PageContent: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <div className="pb-40 px-5 lg:px-14 lg:pl-20 outline-none">
      {children}
    </div>
  );
};

const MarkdocPage: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <Suspense fallback={<PageContent>{children}</PageContent>}>
      <AppContextProvider>
        <PageContent>{children}</PageContent>
      </AppContextProvider>
    </Suspense>
  );
};

export default MarkdocPage;
