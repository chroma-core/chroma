"use client";

import React, { Suspense } from "react";
import { AppContextProvider } from "@/context/app-context";

const MarkdocPage: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <Suspense>
      <AppContextProvider>
        <div className="w-full max-w-full h-full overflow-y-scroll pb-40 px-4 md:pr-4 md:px-14 md:pl-20 prose dark:prose-invert outline-none">
          <div>{children}</div>
        </div>
      </AppContextProvider>
    </Suspense>
  );
};

export default MarkdocPage;