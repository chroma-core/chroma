import * as React from 'react';
import { useRouter } from 'next/router';

// Define the props type
interface HeadingProps {
  id?: string;
  level?: 1 | 2 | 3 | 4 | 5 | 6; // assuming level can only be between 1 and 6
  children: React.ReactNode;
  className?: string;
}

export function Heading({ id = '', level = 1, children, className }: HeadingProps): JSX.Element {
  const router = useRouter();
  const Component = `h${level}` as keyof JSX.IntrinsicElements; // This type assertion helps to ensure that Component is a valid JSX element tag

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


  const link = (
    <Component className={['heading', className].filter(Boolean).join(' ')}>
      <div id={id} />
      {children}
      <a
      onClick={(e) => {
        e.preventDefault();
        navigator.clipboard.writeText(`${window.location.origin}${router.pathname}#${id}`);
        // replacestate with the current url
        window.history.replaceState(null, '', `${window.location.origin}${router.pathname}#${id}`);
      }}
      className="text-gray-500 hover:text-gray-700 ml-2 cursor-pointer href-anchor"
      >
        #
      </a>
    </Component>
  );

  return isDocs && level !== 1 ? (
    <a href={`#${id}`}>
      {link}
    </a>
  ) : (
    link
  );
}
