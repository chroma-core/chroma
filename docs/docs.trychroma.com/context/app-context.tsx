import React, { createContext, useState, ReactNode, useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { getLocalStoragePreferredLanguage, PreferredLanguage, setLocalStoragePreferredLanguage } from "@/lib/utils";


export interface AppContextValue {
  language: string;
  setLanguage: (language: PreferredLanguage) => void;
}

const AppContextDefaultValue: AppContextValue = {
  language: getLocalStoragePreferredLanguage(),
  setLanguage: () => {},
};

const AppContext = createContext<AppContextValue>(AppContextDefaultValue);

export const AppContextProvider = ({ children }: { children: ReactNode }) => {
  const params = useSearchParams();
  const paramsLanguage = params.get("lang") as PreferredLanguage | null;
  const [language, setLanguage] = useState<PreferredLanguage>(
    // due to hydration issues, we can't use the local storage value directly here
    paramsLanguage || "python"
  );
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    const anchor =
      pathname === window.location.pathname ? window.location.hash : "";

    if (paramsLanguage === "typescript") {
      router.replace(`${pathname}?lang=typescript${anchor}`);
      setLocalStoragePreferredLanguage("typescript");
    } else if (paramsLanguage === "python") {
      router.replace(pathname + anchor);
      setLocalStoragePreferredLanguage("python");
    } else {
      router.replace(pathname + anchor);
    }
  }, [paramsLanguage, pathname]);

  useEffect(() => {
    const language = getLocalStoragePreferredLanguage();
    setLanguage(language);
  }, []);

  return (
    <AppContext.Provider value={{ language, setLanguage: (language: string) => {
        if (language === "python" || language === "typescript") {
          setLanguage(language);
          setLocalStoragePreferredLanguage(language);
        }
      }
    }}>
      {children}
    </AppContext.Provider>
  );
};

export default AppContext;
