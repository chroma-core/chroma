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
    console.log(scrollRef.current);
    if (scrollRef.current) {
      sessionStorage.setItem(
        "sidebarScrollPosition",
        scrollRef.current.scrollTop.toString(),
      );
    }
  };

  useEffect(() => {
    const sectionScrollPosition = (userPath: string[]) => {
      const userPage = userPath[userPath.length - 1];
      const currentPage = pagesIndex.find((p) => p === userPage);
      if (!currentPage) return 0;
      return pagesIndex.indexOf(currentPage) * 25;
    };

    if (!scrollRef.current) return;

    const userPath = pathname?.slice(1).split("/");
    if (!userPath) return;

    const section = userPath[0];

    const storedScrollPosition = sessionStorage.getItem(
      "sidebarScrollPosition",
    );

    const storedSection = sessionStorage.getItem("sidebarSection");

    if (!storedSection) {
      sessionStorage.setItem("sidebarSection", section);
    } else if (storedSection !== section) {
      sessionStorage.setItem("sidebarSection", section);
      sessionStorage.removeItem("sidebarScrollPosition");
    }

    if (storedScrollPosition) {
      scrollRef.current.scrollTop = parseInt(storedScrollPosition, 10);
    } else {
      scrollRef.current.scrollTop = sectionScrollPosition(userPath);
    }

    const ref = scrollRef.current;
    ref.addEventListener("scroll", handleScroll);
    return () => ref.removeEventListener("scroll", handleScroll);
  }, [pathname, pagesIndex]);

  return (
    <div
      ref={scrollRef}
      className="flex flex-col h-full pb-10 pt-5 px-5 overflow-y-auto"
    >
      <div className="flex flex-col gap-5">{children}</div>
    </div>
  );
};

export default ScrollableContent;
