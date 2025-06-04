import React, { createContext, useState, ReactNode, useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

export type SidebarState = 'open' | 'closed';

export interface AppContextValue {
  sidebarState: SidebarState;
  language: string;
  setLanguage: (language: string) => void;
}

const AppContextDefaultValue: AppContextValue = {
  sidebarState: 'open',
  language: "python",
  setLanguage: () => {},
};

const AppContext = createContext<AppContextValue>(AppContextDefaultValue);

export const AppContextProvider = ({ children }: { children: ReactNode }) => {
  const searchParams = useSearchParams();
  const [language, setLanguage] = useState<string>(
    searchParams?.get("lang") || "python",
  );
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    const anchor =
      pathname === window.location.pathname ? window.location.hash : "";

    if (language === "typescript") {
      router.replace(`${pathname}?lang=typescript${anchor}`);
    } else {
      router.replace(pathname + anchor);
    }
  }, [language, pathname]);

  return (
    <AppContext.Provider value={{ language, setLanguage, sidebarState: 'open' }}>
      {children}
    </AppContext.Provider>
  );
};

export default AppContext;
