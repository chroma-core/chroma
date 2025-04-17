import { ForwardRefExoticComponent, RefAttributes } from "react";

export interface AppPage {
  id: string;
  name: string;
  slug?: string;
  path?: string;
}

export interface AppSection {
  id: string;
  name: string;
  default?: string;
  icon?: ForwardRefExoticComponent<
    { className?: string } & RefAttributes<SVGSVGElement>
  >;
  pages?: AppPage[];
  generatePages?: boolean;
  subsections?: AppSection[];
  tag?: string;
  disable?: boolean;
}

export const getAllPages = (sidebarConfig: AppSection[], sectionId: string) => {
  const section = sidebarConfig.find((section) => section.id === sectionId);
  if (!section) {
    return [];
  }

  const pages: { id: string; name: string; slug: string }[] = [];

  pages.push(
    ...(section.pages?.map((page) => {
      // Create the slug first, then reuse it for the path
      const pageSlug = `${section.id}/${page.id}`;
      return {
        ...page,
        slug: pageSlug,
        path: `/${pageSlug}`,
      };
    }) || []),
  );

  section.subsections?.forEach((subsection) => {
    pages.push(
      ...(subsection.pages?.map((page) => {
        // Create the slug first, then reuse it for the path
        const pageSlug = `${section.id}/${subsection.id}/${page.id}`;
        return {
          ...page,
          slug: pageSlug,
          path: `/${pageSlug}`,
        };
      }) || []),
    );
  });

  return pages;
};

export const getPagePrevNext = (
  slug: string[],
  pages: AppPage[],
): {
  prev?: AppPage;
  next?: AppPage;
} => {
  const page = slug.join("/");
  const pageIndex = pages.map((page) => page.slug).indexOf(page);
  if (pageIndex === -1) {
    return { prev: undefined, next: undefined };
  }
  if (pageIndex === pages.length - 1) {
    return { prev: pages[pageIndex - 1], next: undefined };
  }
  if (pageIndex === 0) {
    return { prev: undefined, next: pages[pageIndex + 1] };
  }
  return { prev: pages[pageIndex - 1], next: pages[pageIndex + 1] };
};
