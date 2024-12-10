import * as React from 'react';
import { useRouter } from 'next/router';

// Define the props type
interface HeadingProps {
  id?: string;
  level?: 1 | 2 | 3 | 4 | 5 | 6; // assuming level can only be between 1 and 6
  children: React.ReactNode;
  className?: string;
}

export function Heading({ id = '', level = 1, children, className }: HeadingProps) {
  const router = useRouter();
  const Component = `h${level}`;

  const isDocs = router.pathname.startsWith('/docs');

  if (level === 1) {
    className = 'text-3xl mb-6 mt-8 font-semibold';
  }
  if (level === 2) {
    className = 'text-2xl mb-4 mt-6 font-semibold';
  }
  if (level === 3) {
    className = 'text-xl mb-4 mt-4 font-semibold';
  }
  if (level === 4) {
    className = 'text-lg mb-4 mt-4 font-semibold';
  }
  if (level === 5) {
    className = 'text-md mb-4 mt-4 font-semibold';
  }
  if (level === 6) {
    className = 'text-sm mb-4 mt-4 font-semibold';
  }

  const link = React.createElement(
      Component,
      { className: ['heading', className].filter(Boolean).join(' ') },
      React.createElement('div', { id }),
      children,
      React.createElement(
          'a',
          {
            onClick: (e) => {
              e.preventDefault();
              navigator.clipboard.writeText(`${window.location.origin}${router.pathname}#${id}`);
              // Replace state with the current URL
              window.history.replaceState(null, '', `${window.location.origin}${router.pathname}#${id}`);
            },
            className: 'text-gray-500 hover:text-gray-700 ml-2 cursor-pointer href-anchor',
          },
          '#'
      )
  );

  return isDocs && level !== 1
      ? React.createElement('a', { href: `#${id}` }, link)
      : link;
}
