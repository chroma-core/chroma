import React, { useEffect, useState } from 'react';
import Link from 'next/link';

interface TOCItem {
  id: string;
  level: number;
  title: string;
}

interface TableOfContentsProps {
  toc: TOCItem[];
}

export function TableOfContents({ toc }: TableOfContentsProps) {

  const [items, setItems] = useState<TOCItem[]>([]);

  useEffect(() => {
    setItems(toc);
  }, [toc]);

  // for each item, look for it in the DOM and make sure that it is not hidden
  // this enables hiding headings that are hidden from tab state
  let visibleItems = [];
  items.forEach((item) => {
    const el = document.getElementById(item.id);
    // if el is null, cull from items
    if (el !== null) {
      visibleItems.push(item);
    }
  });

  // get the minimum level across all items to left align well
  const minLevel = Math.min(...items.map((item) => item.level));

  return (
    <nav className="toc w-[300px] pt-1 pl-5 mt-8 border-l pr-3 hidden md:block">
      <div>
      {visibleItems.length > 1 ? (
          visibleItems.map((item, index) => {
            const href = `#${item.id}`;
            const active =
              typeof window !== 'undefined' && window.location.hash === href;
            return (
              <div
                key={index} // Changed key to item.id for uniqueness
                className={[
                  active ? 'active' : undefined,
                  'mb-2'
                ]
                  .filter(Boolean)
                  .join(' ')}
                style={{paddingLeft: `${(item.level - minLevel) * 10}px`}}
              >
                <Link href={href} passHref className='font-normal text-sm'>
                  {item.title}
                </Link>
              </div>
            );
          })
      ) : null}
      </div>
    </nav>
  );
}
