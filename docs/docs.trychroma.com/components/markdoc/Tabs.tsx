import React, { createContext, useContext, useState, ReactNode, useEffect, FC } from 'react';
import { GlobalStateContext } from '../layout/state';
import { Tabs as ShadTabs, TabsList, TabsTrigger } from "../ui/tabs"
import slugify from '@sindresorhus/slugify';

// Define the props for the Tabs component
interface TabsProps {
  labels: string[];
  children: ReactNode;
  group?: string;
  hideTabs?: boolean;
  hideContent?: boolean;
}

export const Tabs: FC<TabsProps> = ({ labels, children, group, hideTabs, hideContent }: TabsProps) => {
  const [currentTab, setCurrentTab] = useState<string>(labels[0]);
  const [labelsInternal, setLabelsInternal] = useState<string[]>(labels);
  const [createdTime, setCreatedTime] = useState<number>(Date.now());

  // set labels on init
  useEffect(() => {
    setLabelsInternal(labels);
  }, []);

  const { globalStateObject, setGlobalStateObject } = useContext(GlobalStateContext);

  if (hideTabs === undefined) {
    hideTabs = false;
  }
  if (hideContent === undefined) {
    hideContent = false;
  }

  // When globalStateObject changes, set the appropriate tab
  useEffect(() => {
    if (group !== undefined) setCurrentTab(globalStateObject[group]);
  }, [globalStateObject]);

  // Function to set the global StateObject and current tab
  const setVal = (label: string) => {
    if (group !== undefined) setGlobalStateObject({... globalStateObject, [group]: label});
    setCurrentTab(label);
  };

  let defaultValue = labels[0] || '';
  if (group) defaultValue = globalStateObject[group];

  return (
    <ShadTabs defaultValue={slugify(defaultValue)} value={slugify(currentTab)} className='mb-5'>
      {hideTabs ? null : (
        <TabsList className='w-100'>
        <ul role="tablist">
          {labels.map((label) => (
            <TabsTrigger key={slugify(label)} value={slugify(label)} onClick={() => setVal(label)}>{label}</TabsTrigger>
          ))}
        </ul>
        </TabsList>
      )}
      {hideContent ? null : <div>{children}</div>}
    </ShadTabs>
  );
};
