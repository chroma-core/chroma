"use client";

import React, { ReactElement, useContext, useRef } from "react";
import {
  Tabs as UITabs,
  TabsContent,
  TabsList,
  TabsTrigger as UITabsTrigger,
} from "@/components/ui/tabs";
import { capitalize, cn } from "@/lib/utils";
import { tabLabelStyle } from "@/components/markdoc/code-block-header";
import AppContext from "@/context/app-context";
import CodeBlock from "@/components/markdoc/code-block";
import { Playfair_Display } from "next/font/google";

export interface TabProps {
  label: string;
  children: React.ReactElement<{ content: string; showHeader: boolean }>;
}

const isValidTab = (child: unknown): child is ReactElement<TabProps> =>
  React.isValidElement(child) &&
  typeof (child.props as TabProps)?.label === "string";

export const TabsTrigger = React.forwardRef<
  React.ElementRef<typeof UITabsTrigger>,
  React.ComponentPropsWithoutRef<typeof UITabsTrigger>
>(({ value, ...props }, ref) => {
  const { setLanguage } = useContext(AppContext);
  const triggerRef = useRef<HTMLButtonElement | null>(null);

  return (
    <UITabsTrigger
      ref={triggerRef}
      value={value}
      {...props}
      onClick={() => {
        setLanguage(value);
      }}
    />
  );
});
TabsTrigger.displayName = "TabsTrigger";

const Tab: React.FC<TabProps> = ({ children }) => {
  return <div>{children}</div>;
};

export const Tabs: React.FC<{ children: ReactElement<TabProps>[] }> = ({
  children,
}) => {
  const { language } = useContext(AppContext);
  const validTabs = React.Children.toArray(children).filter(isValidTab);

  if (validTabs.length === 0) return null;

  return (
    <div className="my-4">
      <UITabs
        defaultValue={validTabs[0].props.label}
        value={language}
        className="flex flex-col mt-2 pb-2"
      >
        <TabsList className="justify-start bg-transparent dark:bg-transparent rounded-none p-0 h-fit border-b border-gray-300 mb-4 dark:border-gray-700">
          {validTabs.map((tab) => (
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
          {validTabs.map((tab) => (
            <TabsContent
              key={`${tab.props.label}-content`}
              value={tab.props.label}
              className="m-0"
            >
              {tab.props.children?.type === CodeBlock
                ? React.cloneElement(tab, {
                  children: React.cloneElement(tab.props.children, {
                    showHeader: false,
                  }),
                })
                : tab}
            </TabsContent>
          ))}
        </div>
      </UITabs>
    </div>
  );
};

export const CustomTabsTrigger = React.forwardRef<
  React.ElementRef<typeof UITabsTrigger>,
  React.ComponentPropsWithoutRef<typeof UITabsTrigger>
>(({ value, ...props }, ref) => {
  const triggerRef = useRef<HTMLButtonElement | null>(null);

  return <UITabsTrigger ref={triggerRef} value={value} {...props} />;
});
CustomTabsTrigger.displayName = "CustomTabsTrigger";

export const CustomTabs: React.FC<{ children: ReactElement<TabProps>[] }> = ({
  children,
}) => {
  const validTabs = React.Children.toArray(children).filter(isValidTab);

  if (validTabs.length === 0) return null;

  return (
    <div className="my-4">
      <UITabs
        defaultValue={validTabs[0].props.label}
        className="flex flex-col mt-2 pb-2"
      >
        <TabsList className="justify-start bg-transparent p-0 h-fit dark:bg-transparent rounded-none border-b border-gray-300 dark:border-gray-700">
          {validTabs.map((tab) => (
            <CustomTabsTrigger
              key={`${tab.props.label}-header`}
              value={tab.props.label}
              className={cn(
                tabLabelStyle,
                "text-sm tracking-normal dark:data-[state=active]:bg-transparent data-[state=active]:border-b data-[state=active]:text-gray-900 dark:data-[state=active]:text-gray-200 data-[state=active]:border-gray-900 dark:data-[state=active]:border-gray-200",
              )}
            >
              {capitalize(tab.props.label)}
            </CustomTabsTrigger>
          ))}
        </TabsList>
        <div>
          {validTabs.map((tab) => (
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
    </div>
  );
};

export default Tab;
