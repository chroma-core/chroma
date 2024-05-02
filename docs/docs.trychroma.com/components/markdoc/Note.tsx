import React, { ReactNode } from 'react';
import { Alert, AlertDescription, AlertTitle } from "../../components/ui/alert"
import { Icons } from '../ui/icons';

interface NoteProps {
  type: 'warning' | 'caution' | 'default' | 'tip';
  title?: string;
  children: ReactNode;
}

export function Note({ type, title, children }: NoteProps) {

  if (!type) type = 'default';

  let iconFlex = 'items-center';
  if (title !== undefined) iconFlex = 'items-start';

  return (
    <Alert variant={type} className={`mt-5 mb-5 border-l-8 shadow-sm`}
    >
      <div className={`flex flex-row ${iconFlex}`}>

      {(type === 'tip')? <Icons.info className={`mr-2 ${(title !== undefined)? 'mt-1': null}`} /> : null}
      {(type === 'default')? <Icons.info className={`mr-2 ${(title !== undefined)? 'mt-1': null}`} /> : null}
      {(type === 'warning')? <Icons.warning className={`mr-2 ${(title !== undefined)? 'mt-1': null}`} /> : null}
      {(type === 'caution')? <Icons.warning className={`mr-2 ${(title !== undefined)? 'mt-1': null}`} /> : null}

      <div>
      {title ? (
        <AlertTitle className='text-md font-semibold mb-2 mt-1'>{title}</AlertTitle>
      ) : null}
      <AlertDescription className='text-md'>
        {children}
      </AlertDescription>
      </div>
      </div>
    </Alert>
  )
}
