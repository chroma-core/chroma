import React, { ReactNode } from 'react';
import { TabsContent } from "../ui/tabs"
import slugify from '@sindresorhus/slugify';

interface TabProps {
  label: string;
  children: ReactNode;
}

export function Tab({ label, children }: TabProps) {
  return <TabsContent value={slugify(label)}>{children}</TabsContent>
}
