import React from "react";
import { Input } from "@/components/ui/input";
import ShadowButton from "@/components/ui/shadow-button";
import { ArrowRight } from "lucide-react";

const InputBox: React.FC = () => {
  return (
    <div className="flex items-center justify-between gap-2 h-12 w-full border border-black py-1 px-2">
      <Input className="border-0 shadow-none rounded-none focus-visible:ring-0" />
      <ShadowButton>
        <ArrowRight className="w-4 h-4" />
      </ShadowButton>
    </div>
  );
};

export default InputBox;
