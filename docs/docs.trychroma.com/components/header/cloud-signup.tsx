"use client";

import React, { useEffect, useState } from "react";
import { Cross2Icon } from "@radix-ui/react-icons";
import ChromaIcon from "../../public/chroma-icon.svg";
import Link from "next/link";
import Image from "next/image";

const SignUpLink: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <Link
      href={"https://www.trychroma.com/signup"}
      target="_blank"
      rel="noopener noreferrer"
      className="underline"
    >
      {children}
    </Link>
  );
};

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

const CloudSignUp: React.FC = () => {
  const [open, setOpen] = useState(false);
  const [imageLoaded, setImageLoaded] = useState(false);

  useEffect(() => {
    const lastClosed = localStorage.getItem("cloudSignupLastClosed");
    const now = Date.now();

    if (!lastClosed || now - parseInt(lastClosed, 10) > ONE_WEEK_MS) {
      setOpen(true);
    }
  }, []);

  const handleDialogClose = () => {
    setOpen(false);
    localStorage.setItem("cloudSignupLastClosed", Date.now().toString());
  };

  return (
    open && (
      <div
        className={`absolute bottom-4 z-20 right-4 bg-white border border-black h-48 w-[400px] flex flex-col gap-0 sm:rounded-none p-0 dark:border-white dark:border dark:bg-gray-950 ${imageLoaded ? "opacity-100" : "opacity-0"}`}
      >
        <div className="relative py-2 px-[3px] h-fit border-b-[1px] border-black dark:border-gray-300 dark:bg-gray-950">
          <div className="flex flex-col gap-0.5">
            {[...Array(7)].map((_, index) => (
              <div
                key={index}
                className="w-full h-[1px] bg-black dark:bg-gray-300"
              />
            ))}
            <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-1 bg-white dark:bg-gray-950 font-mono select-none">
              NEW
            </div>
            <div
              className="absolute right-4 top-[6px] px-1 bg-white dark:bg-gray-950 cursor-pointer"
              onClick={handleDialogClose}
            >
              <div className="flex items-center justify-center bg-white dark:bg-gray-950 border-[1px] border-black disabled:pointer-events-none focus-visible:outline-none">
                <Cross2Icon className="h-5 w-5" />
                <span className="sr-only">Close</span>
              </div>
            </div>
          </div>
        </div>
        <div className="flex gap-5 h-full">
          <div className="flex flex-col gap-3 pt-4 pl-5 ">
            <div className="flex items-center gap-2 select-none">
              <ChromaIcon className="w-10 h-10" />
              <p className="text-lg font-bold">Chroma Cloud</p>
            </div>

            <div className="flex flex-col gap-2 text-sm">
              <p>
                Our fully managed hosted service,{" "}
                <span className="font-bold">Chroma Cloud</span> is here.
              </p>
              <p>
                <SignUpLink>Sign up</SignUpLink> for early access!
              </p>
            </div>
          </div>
          <div className="h-full flex items-start justify-end flex-shrink-0 ">
            <Image
              src="/cloud-art.svg"
              alt="Cloud Art"
              width={128}
              height={155}
              priority
              onLoad={() => setImageLoaded(true)}
            />
          </div>
        </div>
      </div>
    )
  );
};

export default CloudSignUp;
