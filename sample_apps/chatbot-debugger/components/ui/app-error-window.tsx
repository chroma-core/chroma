"use client";

import React from "react";
import ErrorWindow from "@/components/ui/error-window";
import { useAppContext } from "@/context/app-context";

const AppErrorWindow: React.FC<{ overrideErrorMessage?: string }> = ({
  overrideErrorMessage,
}) => {
  const { error, setError } = useAppContext();

  if (!error) return null;

  return (
    <div className="absolute bottom-10 right-3 z-10">
      <ErrorWindow
        message={overrideErrorMessage || error}
        onClick={() => setError("")}
      />
    </div>
  );
};

export default AppErrorWindow;
