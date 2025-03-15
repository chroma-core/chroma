"use client";

import React, { createContext, useState, useContext, ReactNode } from "react";

type AppContextType = {
  sidebarValue: string;
  updateSidebarValue: (value: string) => void;
};

const AppContext = createContext<AppContextType | undefined>(undefined);

export function AppProvider({ children }: { children: ReactNode }) {
  const [sidebarValue, setSidebarValue] = useState("default value");

  const updateSidebarValue = (value: string) => {
    setSidebarValue(value);
  };

  return (
    <AppContext.Provider value={{ sidebarValue, updateSidebarValue }}>
      {children}
    </AppContext.Provider>
  );
}

export function useAppContext() {
  const context = useContext(AppContext);
  if (context === undefined) {
    throw new Error("useAppContext must be used within an AppProvider");
  }
  return context;
}
