"use client";

import React, { useEffect, useState } from "react";
import { Cross2Icon } from "@radix-ui/react-icons";
import ChromaIcon from "../../public/chroma-icon.svg";
import Link from "next/link";
import Image from "next/image";
import Header84 from "@/components/ui/header-84";
import CloseButton84 from "@/components/ui/close-button-84";

const SignUpLink: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <Link
      href={"https://www.trychroma.com/signup?utm_source=cloud-signup"}
      target="_blank"
      rel="noopener noreferrer"
      className="underline underline-offset-4 font-semibold"
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
        className={`hidden md:block fixed bottom-4 z-20 right-4 bg-white border border-black h-48 w-[400px] flex flex-col gap-0 sm:rounded-none p-0 dark:border-white dark:border dark:bg-gray-950 ${imageLoaded ? "opacity-100" : "opacity-0"}`}
      >
        <Header84 title="NEW">
          <CloseButton84 onClick={handleDialogClose} />
        </Header84>
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
                <SignUpLink>Sign up →</SignUpLink>
              </p>
            </div>
          </div>
          <div className="h-full flex items-start justify-end flex-shrink-0 ">
            <Image
              src="/cloud-art.jpg"
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
