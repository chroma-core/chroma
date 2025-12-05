import React, { ReactElement } from "react";
import CopyButton from "@/components/markdoc/copy-button";
import { TabsContent, TabsList } from "@/components/ui/tabs";
import { tabLabelStyle } from "@/components/markdoc/code-block-header";
import { capitalize, cn } from "@/lib/utils";
import CodeBlock from "@/components/markdoc/code-block";
import { TabProps, TabsTrigger } from "@/components/markdoc/tabs";
import CodeTabs from "@/components/markdoc/code-tab";

type AnyProps = Record<string, unknown>;

const findCodeBlock = (
  children: React.ReactNode,
): React.ReactElement | null => {
  if (!children) return null;
  if (React.isValidElement(children) && children.type === CodeBlock) {
    return children;
  }
  if (Array.isArray(children)) {
    for (const child of children) {
      const found = findCodeBlock(child);
      if (found) return found;
    }
  }
  if (React.isValidElement(children)) {
    const props = children.props as AnyProps;
    if (props.children) {
      return findCodeBlock(props.children as React.ReactNode);
    }
  }
  return null;
};

const TabbedCodeBlock: React.FC<{
  children: ReactElement<TabProps>[];
}> = ({ children }) => {
  const tabs = children.map((tab) => {
    const codeBlock = findCodeBlock(tab.props.children);
    const codeBlockProps = codeBlock?.props as AnyProps | undefined;
    return {
      label: tab.props.label,
      content: (codeBlockProps?.content as string) || "",
      render: tab,
    };
  });

  return (
    <CodeTabs>
      <div className="flex items-center justify-between bg-gray-900 rounded-t-sm">
        <TabsList className="bg-transparent dark:bg-transparent rounded-none p-0 h-fit m-0">
          {tabs.map((tab) => (
            <TabsTrigger
              key={`${tab.label}-header`}
              value={tab.label}
              className={cn(tabLabelStyle)}
            >
              {capitalize(tab.label)}
            </TabsTrigger>
          ))}
        </TabsList>
        <div className="flex items-center pr-3">
          {tabs.map((tab) => (
            <TabsContent
              key={`${tab.label}-copy`}
              value={tab.label}
              className="flex items-center m-0"
            >
              <CopyButton content={tab.content || ""} />
            </TabsContent>
          ))}
        </div>
      </div>
      <div className="[&_.code-block-header]:hidden">
        {tabs.map((tab) => (
          <TabsContent
            key={`${tab.label}-content`}
            value={tab.label}
            className="m-0"
          >
            {tab.render}
          </TabsContent>
        ))}
      </div>
    </CodeTabs>
  );
};

export default TabbedCodeBlock;
