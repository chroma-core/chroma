import type { Metadata } from "next";
import { PT_Serif } from "next/font/google";
import "./globals.css";

import Search from "@/components/ui/common/search";

const ptSerif = PT_Serif({
  variable: "--font-pt-serif",
  subsets: ["latin"],
  weight: ["400", "700"],
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
        className={`${ptSerif.variable} antialiased`}
      >
        <div className="flex flex-col items-center py-20">
          <div className="w-[600px] max-w-[calc(100dvw-32px)]">
            <Search />
            {children}
          </div>
        </div>
      </body>
    </html>
  );
}
