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
