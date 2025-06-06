import type { Metadata } from "next";
import { Gelasio } from "next/font/google";
import "./globals.css";

import Search from "@/components/search";

const gelasio = Gelasio({
  variable: "--font-gelasio",
  subsets: ["latin"],
  weight: ["400", "500", "600"],
});

export const metadata: Metadata = {
  title: "Microblog with AI Assistant",
  description: "",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={`${gelasio.variable} antialiased`}
      >
        <Search />
        {children}
      </body>
    </html>
  );
}
