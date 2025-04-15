import type { Metadata } from "next";
import "./globals.css";
import React from "react";
import ThemeProvider from "@/components/ui/theme-provider";
import { Inter } from "next/font/google";
import Header from "@/components/header/header";
import PostHogProvider from "@/components/posthog/posthog-provider";
import CloudSignUp from "@/components/header/cloud-signup";
import HeaderNav from "@/components/header/header-nav";

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

const inter = Inter({ subsets: ["latin"] });

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full overscroll-none" suppressHydrationWarning>
      <body className={`h-full overflow-hidden ${inter.className} antialiased`}>
        <ThemeProvider
          attribute="class"
          defaultTheme="system"
          enableSystem
          disableTransitionOnChange
        >
          <PostHogProvider>
            <div className="relative h-full w-full">
              <div className="absolute inset-0 bg-[url('/background.jpg')] bg-cover bg-center opacity-10 dark:invert dark:opacity-10" />
              <div className="relative z-10 flex flex-col h-full">
                <Header />
                <HeaderNav/>
                <CloudSignUp />
                {children}
              </div>
            </div>
          </PostHogProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
