import React, { ReactElement } from "react";
import CopyButton from "@/components/markdoc/copy-button";
import { TabsContent, TabsList } from "@/components/ui/tabs";
import { tabLabelStyle } from "@/components/markdoc/code-block-header";
import { capitalize, cn } from "@/lib/utils";
import CodeBlock from "@/components/markdoc/code-block";
import { TabProps, TabsTrigger } from "@/components/markdoc/tabs";
import CodeTabs from "@/components/markdoc/code-tab";

const TabbedCodeBlock: React.FC<{
  children: ReactElement<TabProps>[];
}> = ({ children }) => {
  const tabs = children.map((tab) => {
    return {
      label: tab.props.label,
      content: tab.props.children.props.content,
      render:
        tab.props.children.type === CodeBlock
          ? React.cloneElement(tab, {
              children: React.cloneElement(tab.props.children, {
                showHeader: false,
              }),
            })
          : tab,
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
      <div>
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
