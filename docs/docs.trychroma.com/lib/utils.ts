import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export const cn = (...inputs: ClassValue[]) => {
  return twMerge(clsx(inputs));
};

export const formatToK = (num: number) => {
  return `${Math.round(num / 1000)}k`;
};

export const capitalize = (str: string) => {
  return `${str.charAt(0).toUpperCase()}${str.slice(1)}`;
};

export type PreferredLanguage = "python" | "typescript";

const LANGUAGE_STORAGE_KEY = "chroma_docs_language";

export const setLocalStoragePreferredLanguage = (language: PreferredLanguage) => {
  if (typeof window !== "undefined") {
    return window.localStorage.setItem(LANGUAGE_STORAGE_KEY, language) 
  }
  return "python";
};

export const getLocalStoragePreferredLanguage = (): PreferredLanguage => {
  if (typeof window !== "undefined") {
    return window.localStorage.getItem(LANGUAGE_STORAGE_KEY) as PreferredLanguage || "python";
  }
  return "python";
}