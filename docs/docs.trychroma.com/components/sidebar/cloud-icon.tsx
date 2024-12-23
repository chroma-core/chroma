import React, {
  ForwardRefExoticComponent,
  RefAttributes,
  forwardRef,
} from "react";

const CloudIcon: ForwardRefExoticComponent<
  React.SVGProps<SVGSVGElement> & RefAttributes<SVGSVGElement>
> = forwardRef<SVGSVGElement, React.SVGProps<SVGSVGElement>>((props, ref) => {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      fill="none"
      viewBox="0 0 24 24"
      strokeWidth={2}
      stroke="currentColor"
      className="size-6"
      ref={ref}
      {...props}
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M2.25 15a4.5 4.5 0 0 0 4.5 4.5H18a3.75 3.75 0 0 0 1.332-7.257 3 3 0 0 0-3.758-3.848 5.25 5.25 0 0 0-10.233 2.33A4.502 4.502 0 0 0 2.25 15Z"
      />
    </svg>
  );
});

export default CloudIcon;
