"use server";

import FeedView from "@/components/feed-view";

export default async function Home() {
  return (
    <div className="flex flex-row justify-center py-20 mb-48">
      <div className="flex flex-col justify-between items-stretch w-[600px] max-w-[calc(100dvw-32px)]">
        <FeedView />
      </div>
    </div>
  );
}
