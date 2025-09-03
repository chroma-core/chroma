"use client";

import React, { useState } from "react";
import Combobox from "@/components/markdoc/combobox";
import Steps from "@/components/markdoc/steps";

export interface ComboboxEntryProps {
  value: string;
  label: string;
  children?: React.ReactNode;
}

export const ComboboxEntry: React.FC<ComboboxEntryProps> = ({ children }) => {
  return <div>{children}</div>;
};

const isComboboxEntry = (
  node: React.ReactNode,
): node is React.ReactElement<ComboboxEntryProps> => {
  return React.isValidElement(node) && node.type === ComboboxEntry;
};

const ComboboxSteps: React.FC<{
  children: React.ReactNode;
  defaultValue?: string;
}> = ({ children, defaultValue }) => {
  const allChildren = React.Children.toArray(children);
  const comboboxEntries = allChildren.filter(isComboboxEntry);
  const options = comboboxEntries.map((entry) => ({
    value: entry.props.value,
    label: entry.props.label,
  }));

  const [activeValue, setActiveValue] = useState<string>(
    defaultValue || (options.length > 0 && options[0].value) || "",
  );

  return (
    <div className="flex flex-col w-full">
      <Combobox
        options={options}
        onSelect={(value: string) => setActiveValue(value)}
        activeValue={activeValue}
      />
      <Steps>
        {allChildren.map((child, i) => {
          if (!isComboboxEntry(child)) {
            return child;
          }
        })}
      </Steps>
    </div>
  );
};

export default ComboboxSteps;
