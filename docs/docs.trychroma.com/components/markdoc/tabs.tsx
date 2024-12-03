import React, { ReactElement } from "react";
import {
  Tabs as UITabs,
  TabsContent,
  TabsList,
  TabsTrigger,
} from "@/components/ui/tabs";
import { capitalize, cn } from "@/lib/utils";
import { tabLabelStyle } from "@/components/markdoc/code-block-header";
import CodeBlock from "@/components/markdoc/code-block";

export interface TabProps {
  label: string;
  children: ReactElement;
}

const Tab: React.FC<TabProps> = ({ children }) => {
  return <div>{children}</div>;
};

export const Tabs: React.FC<{ children: ReactElement<TabProps>[] }> = ({
  children,
}) => {
  return (
    <UITabs defaultValue={children[0].props.label} className="flex flex-col mt-2 border-b-[1px] pb-2">
      <TabsList className="justify-start bg-transparent dark:bg-transparent rounded-none p-0 h-fit border-b border-gray-300 mb-2">
        {children.map((tab) => (
          <TabsTrigger
            key={`${tab.props.label}-header`}
            value={tab.props.label}
            className={cn(
              tabLabelStyle,
              "text-sm tracking-normal dark:data-[state=active]:bg-transparent data-[state=active]:border-b data-[state=active]:text-gray-900 dark:data-[state=active]:text-gray-200 data-[state=active]:border-gray-900 dark:data-[state=active]:border-gray-200",
            )}
          >
            {capitalize(tab.props.label)}
          </TabsTrigger>
        ))}
      </TabsList>
      <div>
        {children.map((tab) => (
          <TabsContent
            key={`${tab.props.label}-content`}
            value={tab.props.label}
            className="m-0"
          >
            {tab}
          </TabsContent>
        ))}
      </div>
    </UITabs>
  );
};

export default Tab;
