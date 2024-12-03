"use client";

import React, { useEffect, useRef } from "react";
import { usePathname } from "next/navigation";

const ScrollableContent: React.FC<{
  pagesIndex: string[];
  children: React.ReactNode;
}> = ({ pagesIndex, children }) => {
  const pathname = usePathname();
  const scrollRef = useRef<HTMLDivElement>(null);

  const handleScroll = () => {
    if (scrollRef.current) {
      sessionStorage.setItem(
        "sidebarScrollPosition",
        scrollRef.current.scrollTop.toString(),
      );
    }
  };

  useEffect(() => {
    const sectionScrollPosition = (pathname: string) => {
      const userPath = pathname.slice(1).split("/");
      const userPage = userPath[userPath.length - 1];

      const currentPage = pagesIndex.find((p) => p === userPage);
      if (!currentPage) {
        return 0;
      }

      return pagesIndex.indexOf(currentPage) * 25;
    };

    if (scrollRef.current) {
      scrollRef.current.scrollTop = sectionScrollPosition(pathname);
    }

    const storedScrollPosition = sessionStorage.getItem(
      "sidebarScrollPosition",
    );

    if (scrollRef.current && storedScrollPosition) {
      scrollRef.current.scrollTop = parseInt(storedScrollPosition, 10);
    }

    if (scrollRef.current) {
      scrollRef.current.addEventListener("scroll", handleScroll);
    }

    return () => {
      scrollRef.current?.removeEventListener("scroll", handleScroll);
    };
  }, [pathname, pagesIndex]);

  return (
    <div
      ref={scrollRef}
      className={`flex flex-col flex-grow overflow-scroll pb-10 pr-5`}
    >
      <div className="flex flex-col gap-5">{children}</div>
    </div>
  );
};

export default ScrollableContent;
