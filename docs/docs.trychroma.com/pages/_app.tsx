import React, { createContext, useContext, useEffect, useState } from 'react';
import Head from 'next/head';
import Link from 'next/link';
import { GlobalStateContext, GlobalStateProvider } from '../components/layout/state';

import posthog from 'posthog-js'
import { PostHogProvider } from 'posthog-js/react'

// Check that PostHog is client-side (used to handle Next.js SSR)
if (typeof window !== 'undefined') {
  posthog.init(process.env.NEXT_PUBLIC_POSTHOG_KEY, {
    api_host: process.env.NEXT_PUBLIC_POSTHOG_HOST || "https://us.i.posthog.com",
    // Enable debug mode in development
    loaded: (posthog) => {
      if (process.env.NODE_ENV === 'development') posthog.debug()
    }
  })
}

import {TopNav} from '../components/layout/TopNav';
import {SideNav} from '../components/layout/SideNav';
import {TableOfContents} from '../components/layout/TableOfContents';

import { ThemeProvider } from 'next-themes'

import '../public/globals.css'
import 'katex/dist/katex.min.css';

import type { AppProps } from 'next/app'
import type { MarkdocNextJsPageProps } from '@markdoc/next.js'

import { Inter, IBM_Plex_Mono } from 'next/font/google'
// import IBM Plex Mono
import { Toaster } from "../components/ui/toaster"
import { Icons } from '../components/ui/icons';
import { ThemeToggle } from '../components/ui/toggle-theme';
import { Breadcrumb, BreadcrumbList, BreadcrumbItem, BreadcrumbLink, BreadcrumbSeparator, BreadcrumbPage } from '../components/ui/breadcrumb';
import { useRouter } from 'next/router';

const inter = Inter({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-inter',
})

const ibmPlexMono = IBM_Plex_Mono({
  subsets: ['latin'],
  display: 'swap',
  weight: "400",
  variable: '--font-ibm-plex-mono',
})

const TITLE = 'Markdoc';
const DESCRIPTION = 'A powerful, flexible, Markdown-based authoring framework';

function collectHeadings(node, sections = []) {
  if (node) {
    if (node.name === 'Heading') {
      const title = node.children[0];

      if (typeof title === 'string') {
        sections.push({
          ...node.attributes,
          title
        });
      }
    }

    if (node.children) {
      for (const child of node.children) {
        collectHeadings(child, sections);
      }
    }
  }

  return sections;
}

const ROOT_ITEMS = require(`./_sidenav.js`).items;

export type ChromaDocsProps = MarkdocNextJsPageProps

export default function ChromaDocs({ Component, pageProps }: AppProps<ChromaDocsProps>) {
  const { markdoc } = pageProps;

  const router = useRouter()

  useEffect(() => {
    // Track page views
    const handleRouteChange = () => posthog?.capture('$pageview')
    router.events.on('routeChangeComplete', handleRouteChange)

    return () => {
      router.events.off('routeChangeComplete', handleRouteChange)
    }
  }, [])

  let title = TITLE;
  let description = DESCRIPTION;
  if (markdoc && (markdoc.frontmatter !== undefined)) {
    if (markdoc.frontmatter.title) {
      title = markdoc.frontmatter.title;
    }
    if (markdoc.frontmatter.description) {
      description = markdoc.frontmatter.description;
    }
  }

  const toc = pageProps.markdoc?.content
    ? collectHeadings(pageProps.markdoc.content)
    : [];

  // get the pathname and figure out if we are in a group or not
  // this requires iterating through ROOT_ITEMS
  const pathname = router.pathname;
  let pathVar = router.asPath.split('/')[1] !== undefined ? router.asPath.split('/')[1] : '';
  let inGroup = false
  let groupObject = null
  // iterate through ROOT_ITEMS to see if pathVar is a group or not
  ROOT_ITEMS.map((item: any) => {
    if (item.href === `/${pathVar}` && item.type === 'group') {
      inGroup = true
      groupObject = item
    }
  })

  // if inGroup is true, then we need to build the breadcrumbs
  let breadcrumbs = []
  if (inGroup) {
    breadcrumbs.push({href: `/${pathVar}`, name: groupObject.title})
  }
  // if markdoc.frontmatter.title does not equal the groupObject.title, then we need to add it to our breadcrumbs
  // if the route is more than 1 level deep
  if (router.pathname.split('/').length > 2) {
    if (inGroup && (markdoc.frontmatter !== undefined) && markdoc.frontmatter.title !== groupObject.title) {
      breadcrumbs.push({href: `/${pathVar}/${markdoc.frontmatter.title}`, name: markdoc.frontmatter.title})
    }
  }

  const pageTitle = `${title}${title !== "Chroma" ? " | Chroma Docs": " Docs" }`


  // generate the github edit link
  let filePath = router.asPath.split('/').slice(1).join('/')

  // if root, then index.md
  if (filePath === '') {
    filePath = 'index'
  }

  // if inGroup but .slice(2) is empty, then add index.md
  if (inGroup && router.asPath.split('/').slice(1)[1] === undefined) {
    filePath = filePath + '/index'
  }

  let githubEditLink = `https://github.com/chroma-core/chroma/blob/main/docs/docs.trychroma.com/pages/` + filePath + '.md'

  return (
    <PostHogProvider client={posthog}>
    <ThemeProvider>
    <main className={`${inter.variable} font-sans ${ibmPlexMono.variable}`} style={{paddingBottom: '200px'}}>
      <GlobalStateProvider>
        <Head>
          <title>{pageTitle}</title>
          <meta name="viewport" content="width=device-width, initial-scale=1.0" />
          <meta name="referrer" content="strict-origin" />
          <meta name="title" content={title} />
          <meta name="description" content={description} />
          <link rel="shortcut icon" href="/img/favicon.ico" />
          <link rel="icon" href="/img/favicon.ico" />
          <link
            rel="preconnect"
            href={`https://${process.env.NEXT_PUBLIC_ALGOLIA_APP_ID}-dsn.algolia.net`}
            crossOrigin=""
          />
        </Head>
        <TopNav>
          <Link href="https://discord.gg/MMeYNTmh3x" className='hidden sm:block'>
            <img src="https://img.shields.io/discord/1073293645303795742?cacheSeconds=3600"/>
          </Link>
          <Link href="https://github.com/chroma-core/chroma">
            <img src="https://img.shields.io/github/stars/chroma-core/chroma.svg?style=social&label=Star&maxAge=2400"/>
          </Link>
          <Link href="https://twitter.com/trychroma" className='hidden sm:block'>
            <img src="https://img.shields.io/twitter/follow/trychroma"/>
          </Link>
        <ThemeToggle/>
        </TopNav>
        <div className="flex flex-col space-y-8 lg:flex-row lg:space-x-12 lg:space-y-0 justify-between max-w-screen">
          <div className="block mt-[calc(var(--ifm-navbar-height)*-1)] transition-width  w-[var(--doc-sidebar-width)] will-change-width clip-path-[inset(0)]">
            <div className="h-full overflow-y-scroll max-h-screen sticky top-0">
              <SideNav  />
            </div>
          </div>

          <main className="max-w-screen-md article grow p-5 md:p-0 md:pt-8">

          {inGroup? (
          <Breadcrumb className='mb-8'>
            <BreadcrumbList>
              <BreadcrumbItem>
                <BreadcrumbLink href="/">Home</BreadcrumbLink>
              </BreadcrumbItem>
              <BreadcrumbSeparator />
              {breadcrumbs.map((item, index) => {
                return (
                  <>
                  <BreadcrumbItem key={index}>
                    <BreadcrumbLink href={item.href}>{item.name}</BreadcrumbLink>
                  </BreadcrumbItem>
                  {/* if not the last item, add a BreadcrumbSeparator */}
                  {index !== breadcrumbs.length - 1 ? (
                    <BreadcrumbSeparator />
                  ) : (
                    <></>
                  )}
                  </>
                )
              })}

            </BreadcrumbList>
          </Breadcrumb>
          ) : <> </>}

            <div className='text-3xl mb-6 font-semibold'>{title}</div>
            <Component {...pageProps} />
            <div className="mt-20">
              <a
                href={githubEditLink}
                target="_blank"
                rel="noopener noreferrer"
                className="underline font-semibold"
                style={{textUnderlinePosition: 'under', textUnderlineOffset: '0.2em', textDecorationColor: '#bfbfbf'}}
              >
                <Icons.gitHub className="inline-block w-6 h-6 mr-2" />
                Edit this page on GitHub
              </a>
            </div>
          </main>

          <div className="block mt-[calc(var(--ifm-navbar-height)*-1)] transition-width  w-[var(--doc-sidebar-width)] will-change-width clip-path-[inset(0)]"
          style={{margin: '0 !important'}}
          >
            <div className="h-full overflow-y-scroll max-h-screen sticky top-0">
              <TableOfContents toc={toc} />
            </div>
          </div>

        </div>
        <Toaster />
      </GlobalStateProvider>

    </main>
    </ThemeProvider>
    </PostHogProvider>
  );
}
