import React, { ReactElement } from "react";
import { tabLabelStyle } from "@/components/markdoc/code-block-header";
import { capitalize, cn } from "@/lib/utils";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { TabProps } from "@/components/markdoc/tabs";
import CodeBlock from "@/components/markdoc/code-block";
import CopyButton from "@/components/markdoc/copy-button";

const TabbedUseCaseCodeBlock: React.FC<{
  language: string;
  children: ReactElement<TabProps>[];
}> = ({ language, children }) => {
  return (
    <Tabs defaultValue={children[0].props.label} className="flex flex-col">
      <div className="flex items-center justify-between bg-gray-900 rounded-t-sm">
        <div className="flex items-center gap-7">
          <div className={tabLabelStyle} data-state={"active"}>
            {capitalize(language)}
          </div>
          <TabsList className="bg-transparent dark:bg-transparent rounded-none p-0 h-fit">
            {children.map((tab) => (
              <TabsTrigger
                key={`${tab.props.label}-header`}
                value={tab.props.label}
                className={cn(
                  tabLabelStyle,
                  "data-[state=active]:text-gray-200 data-[state=active]:border-gray-200 dark:data-[state=active]:bg-transparent",
                )}
              >
                {tab.props.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </div>
        <div className="flex items-center pr-3">
          {children.map((tab) => (
            <TabsContent
              key={`${tab.props.label}-copy`}
              value={tab.props.label}
              className="flex items-center m-0"
            >
              <CopyButton content={tab.props.children.props.content || ""} />
            </TabsContent>
          ))}
        </div>
      </div>
        {children.map((tab) => (
          <TabsContent
            key={`${tab.props.label}-content`}
            value={tab.props.label}
            className="m-0"
          >
            {tab.props.children.type === CodeBlock
              ? React.cloneElement(tab, {
                  children: React.cloneElement(tab.props.children, {
                    showHeader: false,
                  }),
                })
              : tab}
          </TabsContent>
        ))}
    </Tabs>
  );
};

export default TabbedUseCaseCodeBlock;
