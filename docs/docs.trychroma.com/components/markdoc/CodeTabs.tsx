import React, { createContext, useContext, useState, ReactNode, useEffect, FC } from 'react';
import { GlobalStateContext } from '../layout/state';
import { Tabs as ShadTabs, TabsList, TabsTrigger } from "../ui/tabs"
import slugify from '@sindresorhus/slugify';
import { CustomHeader } from '../CodeBlock';

// Define the props for the Tabs component
interface TabsProps {
  labels: string[];
  children: ReactNode;
  customHeader?: string;
}

export const CodeTabs: FC<TabsProps> = ({ labels, children, customHeader }: TabsProps) => {

  let defaultValue = labels[0] || '';
  if (!customHeader) {
    customHeader = '';
  }

  return (
    <div className='code-tabs mb-5'>
      <CustomHeader language={customHeader} filename={undefined} codetab={false} />
      <ShadTabs defaultValue={slugify(defaultValue)} >
          <TabsList className='w-100'>
          <ul role="tablist">
            {labels.map((label) => (
              <TabsTrigger key={slugify(label)} value={slugify(label)}>{label}</TabsTrigger>
            ))}
          </ul>
          </TabsList>
          {children}
      </ShadTabs>
    </div>
  );
};
