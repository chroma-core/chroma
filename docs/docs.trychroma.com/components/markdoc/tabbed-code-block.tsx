import React, { ReactElement } from "react";
import CopyButton from "@/components/markdoc/copy-button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { tabLabelStyle } from "@/components/markdoc/code-block-header";
import { capitalize, cn } from "@/lib/utils";
import CodeBlock from "@/components/markdoc/code-block";
import { TabProps } from "@/components/markdoc/tabs";


const TabbedCodeBlock: React.FC<{
  children: ReactElement<TabProps>[];
}> =  ({ children }) => {
  return (
    <Tabs
      defaultValue={children[0].props.children.props.language}
      className="flex flex-col mt-5"
    >
      <div className="flex items-center justify-between bg-gray-900">
        <TabsList className="bg-transparent dark:bg-transparent rounded-none p-0 h-fit m-0">
          {children.map((tab) => (
            <TabsTrigger
              key={`${tab.props.label}-header`}
              value={tab.props.label}
              className={cn(tabLabelStyle)}
            >
              {capitalize(tab.props.label)}
            </TabsTrigger>
          ))}
        </TabsList>
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
      <div>
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
      </div>
    </Tabs>
  );
};

export default TabbedCodeBlock;
