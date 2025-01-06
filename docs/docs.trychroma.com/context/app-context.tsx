import React, {
  createContext,
  useState,
  ReactNode,
  useEffect,
} from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

export interface AppContextValue {
  language: string;
  setLanguage: (language: string) => void;
}

const AppContextDefaultValue: AppContextValue = {
  language: "python",
  setLanguage: () => { },
};

const AppContext = createContext<AppContextValue>(AppContextDefaultValue);

export const AppContextProvider = ({ children }: { children: ReactNode }) => {
  const searchParams = useSearchParams();
  const [language, setLanguage] = useState<string>(
    searchParams.get("lang") || "python",
  );
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    // Don't consider the hash if the user is going to a differnet path.
    const anchor = (pathname === window.location.pathname) ? window.location.hash : "";

    if (language === "typescript") {
      router.replace(pathname + "?lang=typescript" + anchor);
    } else {
      router.replace(pathname + anchor);
    }
  }, [language, pathname]);

  return (
    <AppContext.Provider value={{ language, setLanguage }}>
      {children}
    </AppContext.Provider>
  );
};

export default AppContext;
