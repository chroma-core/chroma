import React, { useState, ReactNode } from 'react';
import Link from 'next/link'; // Assuming you're using Next.js Link, make sure to import it.

interface TopNavProps {
  children: ReactNode; // This type accepts anything that can be rendered: numbers, strings, elements or an array containing these types.
}

import '@docsearch/css';
import { DocSearch } from '@docsearch/react';

export function TopNav({ children }: TopNavProps) {
  return (
    <div className={` dark:bg-stone-950 px-2 pr-0 border-b`}>
      <nav>
        <div className="flex h-16 items-center justify-between px-4 ">
          <div className='flex'>
          <Link href="/" className="flex column items-center">
            <img src='/img/chroma.svg' alt='Chroma Logo' className='h-8 w-auto' />
            <p className='ml-3 mb-0 text-lg font-semibold'>Chroma</p>
          </Link>
          <Search/>
          </div>
          <section className={'flex gap-x-4 items-center'}>
            {children}
          </section>
        </div>
      </nav>
    </div>
  );
}


function Search() {
  return (
    <DocSearch
      appId={process.env.NEXT_PUBLIC_ALGOLIA_APP_ID}
      apiKey={process.env.NEXT_PUBLIC_ALGOLIA_API_KEY}
      indexName={process.env.NEXT_PUBLIC_ALGOLIA_INDEX_NAME}
      insights
    />
  );
}
