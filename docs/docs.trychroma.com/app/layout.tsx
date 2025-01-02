import type { Metadata } from "next";
import "./globals.css";
import React from "react";
import ThemeProvider from "@/components/ui/theme-provider";
import { Inter } from "next/font/google";
import Header from "@/components/header/header";
import PostHogProvider from "@/components/posthog/posthog-provider";

export const metadata: Metadata = {
  title: "Chroma Docs",
  description: "Documentation for ChromaDB",
};

const inter = Inter({ subsets: ["latin"] });

export default function RootLayout({
  children,
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
                {children}
              </div>
            </div>
          </PostHogProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
