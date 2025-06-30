"use client";

import { usePathname, useSearchParams } from "next/navigation";
import React, { useEffect, Suspense } from "react";
import { usePostHog } from "posthog-js/react";

export const PostHogPageView: React.FC = () => {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const posthog = usePostHog();

  useEffect(() => {
    if (pathname && posthog) {
      let url = window.origin + pathname;
      if (searchParams?.toString()) {
        url = `${url}?${searchParams.toString()}`;
      }

      posthog.capture("$pageview", { $current_url: url });
    }
  }, [pathname, searchParams, posthog]);

  return null;
};

const SuspendedPostHogPageView: React.FC = () => {
  return (
    <Suspense fallback={null}>
      <PostHogPageView />
    </Suspense>
  );
};

export default SuspendedPostHogPageView;
