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
ComboboxEntry.displayName = "ComboboxEntry";

const isComboboxEntry = (
  node: React.ReactNode,
): node is React.ReactElement<ComboboxEntryProps> => {
  if (!React.isValidElement(node)) return false;

  return (
    node.props &&
    typeof node.props.value === "string" &&
    typeof node.props.label === "string"
  );
};

const ComboboxSteps: React.FC<{
  children: React.ReactNode;
  defaultValue?: string;
  itemType: string;
}> = ({ children, defaultValue, itemType }) => {
  const allChildren = React.Children.toArray(children);
  const comboboxEntries = allChildren.filter(isComboboxEntry);
  const options = comboboxEntries
    .map((entry) => ({
      value: entry.props.value,
      label: entry.props.label,
    }))
    .sort((a, b) => a.label.localeCompare(b.label));

  const [activeValue, setActiveValue] = useState<string>(
    defaultValue || (options.length > 0 && options[0].value) || "",
  );

  return (
    <div className="flex flex-col w-full">
      <Combobox
        options={options}
        onSelect={(value: string) => setActiveValue(value)}
        activeValue={activeValue}
        itemType={itemType}
      />
      <Steps>
        {allChildren
          .filter(
            (child) =>
              !isComboboxEntry(child) || child.props.value === activeValue,
          )
          .map((child) => {
            if (!isComboboxEntry(child)) {
              return child;
            }
            if (child.props.value === activeValue) {
              return React.Children.toArray(child.props.children);
            }
            return null;
          })
          .flat()}
      </Steps>
    </div>
  );
};

export default ComboboxSteps;
