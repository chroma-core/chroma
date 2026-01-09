import type { Metadata, Viewport } from "next";
import "./globals.css";
import React from "react";
import ThemeProvider from "@/components/ui/theme-provider";
import { Inter } from "next/font/google";
import Header from "@/components/header/header";
import PostHogProvider from "@/components/posthog/posthog-provider";
import CloudSignUp from "@/components/header/cloud-signup";
import HeaderNav from "@/components/header/header-nav";
import GTM from "@/components/gtm";

import "@/components/markdoc/code-block-themes.css";

export const metadata: Metadata = {
  title: 'Chroma Docs',
  description: 'Documentation for ChromaDB',
  openGraph: {
    title: 'Chroma Docs',
    description: 'Documentation for ChromaDB',
    siteName: 'Chroma Docs',
    url: 'https://docs.trychroma.com',
    images: [
      {
        url: 'https://docs.trychroma.com/og.png', // must be an absolute url
        width: 2400,
        height: 1256,
      },
    ],
    locale: 'en_US',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'Chroma Docs',
    description: 'Documentation for ChromaDB',
    site: 'trychroma',
    siteId: '1507488634458439685',
    creator: '@trychroma',
    creatorId: '1507488634458439685',
    images: ['https://docs.trychroma.com/og.png'], // must be an absolute url
  },
}

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
}

const inter = Inter({ subsets: ["latin"] });

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  const websiteSchema = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    "@id": "https://docs.trychroma.com/#website",
    "url": "https://docs.trychroma.com/",
    "name": "Chroma Docs",
    "alternateName": "Chroma Documentation",
  };

  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{
            __html: JSON.stringify(websiteSchema),
          }}
        />
      </head>
      <body data-invert-bg="true" className={`${inter.className} antialiased bg-white dark:bg-black bg-[url(/composite_noise.jpg)] bg-repeat relative text-[#27201C] dark:text-white dark:backdrop-invert`}>
        <GTM />
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <PostHogProvider>
            {/* the primary page structure is all done here
                first we make the page a large flex column container */}
            <div className="relative z-10 flex flex-col h-dvh overflow-hidden">
              {/* prevent the header from shrinking */}
              <div className="shrink-0">
                <Header />
                <HeaderNav />
              </div>
              {/* have this container take up the remaining space and hide any overflow
                  the side bar and main page content will be rendered here and will
                  fill the available space and do their own scrolling */}
              <div className="flex-1 overflow-y-hidden h-full">
                {children}
              </div>
            </div>
            {/* the cloud signup can live down here as it is position fixed */}
            <CloudSignUp />
          </PostHogProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
