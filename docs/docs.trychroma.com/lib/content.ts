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
  comingSoon?: boolean;
  override?: string;
}

export const getAllPages = (sidebarConfig: AppSection[], sectionId: string) => {
  const section = sidebarConfig.find((section) => section.id === sectionId);
  if (!section) {
    return [];
  }

  const pages: { id: string; name: string; slug: string }[] = [];

  pages.push(
    ...(section.pages?.map((page) => {
      return {
        ...page,
        slug: `${section.id}/${page.id}`,
        path: `./${page.slug}`,
      };
    }) || []),
  );

  section.subsections?.forEach((subsection) => {
    pages.push(
      ...(subsection.pages?.map((page) => {
        return {
          ...page,
          slug: `${section.id}/${subsection.id}/${page.id}`,
          path: `../${subsection.id}/${page.id}`,
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
