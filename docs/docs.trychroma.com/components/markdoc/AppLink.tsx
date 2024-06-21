import React from 'react';
import Link from 'next/link';

export function AppLink(props) {
  return (
    <Link {...props} passHref className='underline font-semibold' style={{textUnderlinePosition: 'under'}}>
      {props.children}
    </Link>
  );
}
