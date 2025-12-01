import Chat from "@/components/chat";
import Search from "@/components/search";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import Image from "next/image";
import chromaWordmark from "/public/chroma-wordmark.svg";

export default function Home() {
  return (
    <div className="flex h-screen items-center justify-center overflow-hidden bg-zinc-100 font-sans">
      <main className="grid h-screen w-full max-w-5xl grid-rows-[auto_1fr] flex-col overflow-hidden bg-white px-16">
        <header className="flex flex-row items-center py-6">
          <Image src={chromaWordmark} alt="Chroma" className="w-30" />
        </header>

        <Tabs defaultValue="search" className="overflow-hidden">
          <TabsList className="w-full">
            <TabsTrigger value="search">Search</TabsTrigger>
            <TabsTrigger value="chat">Chat</TabsTrigger>
          </TabsList>

          <TabPanel value="search">
            <Search />
          </TabPanel>
          <TabPanel value="chat">
            <Chat />
          </TabPanel>
        </Tabs>
      </main>
    </div>
  );
}

function TabPanel({
  children,
  value,
}: {
  children: React.ReactNode;
  value: string;
}) {
  return (
    <TabsContent
      value={value}
      className="flex flex-auto flex-col overflow-hidden"
    >
      {children}
    </TabsContent>
  );
}
