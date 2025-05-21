"use client";
import { createContext, Dispatch, SetStateAction, useContext, useState } from "react";

type SearchEngineContextType = {
    hostUrl: string | null;
    setHostUrl: (url: string | null) => void;
    query: string | null;
    setQuery: (query: string | null) => void;
};

const SearchEngineContext = createContext<SearchEngineContextType | undefined>(undefined);

export function SearchEngineContextProvider({ children }: { children: React.ReactNode }) {
    const [hostUrl, setHostUrl] = useState<string | null>(null);
    const [query, setQuery] = useState<string | null>(null);

    return (
        <SearchEngineContext.Provider value={{ hostUrl, setHostUrl, query, setQuery }}>
            {children}
        </SearchEngineContext.Provider>
    );
}

export function useSearchEngineContext() {
    const context = useContext(SearchEngineContext);
    if (context === undefined) {
        throw new Error("useCounter must be used within a CounterProvider");
    }
    return context;
}