"use client";

import React from "react";
import { DocSearch } from "@docsearch/react";
import "@docsearch/css";

const SearchBox: React.FC = () => {
  return (
    <DocSearch
      appId={process.env.NEXT_PUBLIC_ALGOLIA_APP_ID!}
      apiKey={process.env.NEXT_PUBLIC_ALGOLIA_API_KEY!}
      indexName={process.env.NEXT_PUBLIC_ALGOLIA_INDEX_NAME!}
      insights
    />
  );
};

export default SearchBox;
