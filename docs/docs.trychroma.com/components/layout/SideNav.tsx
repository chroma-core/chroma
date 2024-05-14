import React from 'react';
import { useRouter } from 'next/router';
import Link from 'next/link';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "../ui/dropdown-menu"

interface LinkItem {
  href: string;
  children: React.ReactNode;
}

interface SideNavItem {
  title: string;
  links: LinkItem[];
}

interface SidebarNavItemFlat {
  title: string;
  href: string;
  children: React.ReactNode;
  type: string;
}

const ROOT_ITEMS = require(`../../pages/_sidenav.js`).items;

export function SideNav() {
  const router = useRouter();

  // Dynamically import the sidenav configuration based on the current path
  // Assuming the items are of type SideNavItem[]
  let pathVar = router.asPath.split('/')[1] !== undefined ? router.asPath.split('/')[1] : '';
  pathVar = pathVar.split('?')[0];

  let group = false
  let groupObject = null
  // iterate through ROOT_ITEMS to see if pathVar is a group or not
  // this tell us whether to show the root items or the group items
  ROOT_ITEMS.map((item: SidebarNavItemFlat) => {
    if (item.href === `/${pathVar}` && item.type === 'group') {
      group = true
      groupObject = item
    }
  })

  pathVar = (pathVar !== "") ? pathVar + '/' : pathVar;
  if (!group) pathVar = '';
  const items: (SideNavItem[] | SidebarNavItemFlat[]) = require(`../../pages/${pathVar}_sidenav.js`).items;

  const navToURL = (url: string) => {
    router.push(url);
  }

  return (
    <>

    <nav className="min-w-[300px] px-3 py-4">

      {(group) ? (
        <div className='mb-3'>
          <SideNavLink href={'/'} title='<-- Home' />
          <SideNavLink href={groupObject.href} title={groupObject.title} />
        </div>
      ) : (
        <></>
      )}

      <div className='block md:hidden'>
      <DropdownMenu>
        <DropdownMenuTrigger className='w-screen flex pr-5'>
          <div className='text-lg px-5 py-3 rounded-md grow border border-input bg-background shadow-sm hover:bg-accent hover:text-accent-foreground'>Menu</div>
        </DropdownMenuTrigger>
        <DropdownMenuContent>
            {items.map((item: any) => (
              item.links ? (
                <div key={item.title} className=''>
                  <DropdownMenuLabel>{item.title}</DropdownMenuLabel>
                    {item.links.map((link: LinkItem, index) => {
                      return (
                        <DropdownMenuItem key={index} onMouseDown={() => navToURL(link.href)} className={`${router.pathname === link.href ? 'active' : ''}`}>
                            {link.children}
                        </DropdownMenuItem>
                      );
                    })}
                </div>
              ) : (
                // if the item is item.type == 'group' then add class category-link
                <div key={item.title} className={`${item.type === 'group' ? 'category-link' : ''}`}>
                  <DropdownMenuItem onMouseDown={() => navToURL(item.href)}>{item.title}</DropdownMenuItem>
                </div>
              )
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className='hidden md:block'>
        {items.map((item: any) => (
          item.links ? (
            <div key={item.title} className='mt-10'>
              <div className='font-bold uppercase text-xs mb-2 ml-5'>{item.title}</div>
                {item.links.map((link: LinkItem, index) => {
                  return (
                    <SideNavLink key={index} href={link.href} title={link.children}/>
                  );
                })}
            </div>

          ) : (
            // if the item is item.type == 'group' then add class category-link
            <div key={item.title} className={`${item.type === 'group' ? 'category-link' : ''}`}>
              <SideNavLink href={item.href} title={item.title} />
            </div>
          )
        ))}
      </div>
    </nav>
    </>
  );
}

// SideNav Link component
export const SideNavLink = ({ href, title }) => {
  const router = useRouter();
  const activeClass = `sideNavItemActive rounded-md font-medium text-md`
  return (
    <Link
      key={href}
      href={href}
      className={`block px-5 sideNavItem cursor-pointer rounded-md  text-md ${router.pathname === href ? activeClass : ''}`}
      style={{fontSize: '17px', paddingTop: '6.375px', paddingBottom: '6.375px'}}
    >
    {title}
  </Link>
  );
}
