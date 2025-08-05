"use client";

import posthog from "posthog-js";
import { PostHogProvider as PHProvider } from "posthog-js/react";
import React, { useEffect } from "react";
import SuspendedPostHogPageView from "@/components/posthog/posthog-pageview";

const PostHogProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  useEffect(() => {
    posthog.init(process.env.NEXT_PUBLIC_POSTHOG_KEY!, {
      api_host: '/c5/',
      ui_host: 'https://us.posthog.com',
      person_profiles: "identified_only",
      capture_pageview: true,
    });
  }, []);

  return (
    <PHProvider client={posthog}>
      <SuspendedPostHogPageView />
      {children}
    </PHProvider>
  );
};

export default PostHogProvider;
