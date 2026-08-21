import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import React from "react";
import Header from "@/components/header/header";
import AppLayout from "@/components/ui/app-layout";
import ErrorWindow from "@/components/ui/error-window";
import { appSetup } from "@/lib/server-utils";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Chroma - Chatbot Debugger",
};

const RootLayout = async ({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) => {
  const setUpResult = await appSetup();

  return (
    <html lang="en">
      <body
        className={`h-screen w-full overflow-hidden ${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        <div className="relative h-full w-full">
          <div className="absolute inset-0 bg-[url('/background.jpg')] bg-cover bg-center opacity-10" />
          <div className="relative z-10 flex flex-col h-full">
            <Header />
            <div className="flex items-center justify-center w-full h-full">
              <div className="relative flex items-center justify-center w-full h-full">
                {!setUpResult.ok && (
                  <ErrorWindow message={setUpResult.error!.message} />
                )}
                {setUpResult.ok && <AppLayout>{children}</AppLayout>}
              </div>
            </div>
          </div>
        </div>
      </body>
    </html>
  );
};

export default RootLayout;
