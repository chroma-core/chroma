import React from "react";
import { Button, buttonVariants } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { VariantProps } from "class-variance-authority";

interface UIButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  children?: React.ReactNode;
  asChild?: boolean;
}

const UIButton = React.forwardRef<HTMLButtonElement, UIButtonProps>(
  ({ children, className, variant, size, ...props }, ref) => {
    const customStyles = cn(
      "flex items-center justify-center border-x-[0.9px] border-y-[1px] shadow outline-none h-full rounded py-[0.2rem]",
      "text-[#27201C] bg-gradient-to-b from-[#FFFFFF] to-[#f9f9f9] border-[#171716]/40 hover:bg-gradient-to-b hover:from-gray-100 hover:to-gray-100",
      "dark:text-[#fff] dark:bg-gradient-to-b dark:from-[#171716] dark:to-[#171716] border-[0.8px] dark:border-[#fff]/40 dark:hover:from-[#171716]/90 dark:hover:to-[#171716]/90",
      className,
    );

    return (
      <Button
        ref={ref}
        variant={variant}
        size={size}
        className={customStyles}
        {...props}
      >
        {children}
      </Button>
    );
  },
);
UIButton.displayName = "UIButton";

export default UIButton;
