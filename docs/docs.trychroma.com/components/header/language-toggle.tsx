"use client";

import React, { useState } from "react";
import PythonLogo from "../../public/python.svg";
import TypeScriptLogo from "../../public/typescript.svg";
import UIButton from "@/components/ui/ui-button";

const supportedLanguages: {
  [language: string]: React.FC<React.SVGProps<SVGSVGElement>>;
} = {
  python: PythonLogo,
  typescript: TypeScriptLogo,
};

const LanguageToggle: React.FC = () => {
  const [preferredLanguage, setPreferredLanguage] = useState<string>("python");

  const switchLanguage = (language: string) => {
    setPreferredLanguage(language);
  };

  return (
    <div className="flex items-center gap-2">
      {Object.keys(supportedLanguages).map((language) => {
        const Logo = supportedLanguages[language];
        const selected = preferredLanguage === language;
        return (
          <UIButton
            key={`${language}-button`}
            className={`p-[0.35rem] ${!selected && "grayscale"}`}
            onClick={switchLanguage.bind(null, language)}
          >
            <Logo className="h-4 w-4" />
          </UIButton>
        );
      })}
    </div>
  );
};

export default LanguageToggle;
