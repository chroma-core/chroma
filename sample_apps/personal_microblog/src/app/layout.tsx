import type { Metadata } from "next";
import { Exo_2 } from "next/font/google";
import "./globals.css";

import Search from "@/components/ui/common/search";

const exo2 = Exo_2({
  variable: "--font-exo-2",
  style: ["normal", "italic"],
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
        className={`${exo2.variable} antialiased bg-[var(--background)]`}
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
