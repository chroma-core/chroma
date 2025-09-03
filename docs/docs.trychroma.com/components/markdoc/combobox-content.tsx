"use client";

import React from "react";
import Combobox from "@/components/markdoc/combobox";

export const ComboboxEntry: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  return <div>{children}</div>;
};
ComboboxEntry.displayName = "ComboboxEntry";

const ComboboxContent: React.FC = () => {
  return (
    <div className="flex flex-col w-full">
      <Combobox
        options={[
          { value: "one", label: "One" },
          { value: "two", label: "Two" },
        ]}
        onSelect={(value: string) => {}}
        activeValue={"one"}
      />
    </div>
  );
};

export default ComboboxContent;
