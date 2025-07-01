import React from "react";
import {
  Accordion as UIAccordion,
  AccordionContent,
  AccordionItem as UIAccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

interface AccordionItemProps {
  value: string;
  label: string;
  children: React.ReactNode;
}

export const AccordionItem: React.FC<AccordionItemProps> = ({
  value,
  label,
  children,
}) => {
  return (
    <UIAccordionItem value={value}>
      <AccordionTrigger className="m-0 p-0">{label}</AccordionTrigger>
      <AccordionContent className="m-0 p-0">{children}</AccordionContent>
    </UIAccordionItem>
  );
};

const Accordion: React.FC<{
  children:
    | React.ReactElement<AccordionItemProps>[]
    | React.ReactElement<AccordionItemProps>;
}> = ({ children }) => {
  return (
    <UIAccordion type="single" collapsible className="w-full">
      {!Array.isArray(children) && (
        <AccordionItem value="item-0" label={children.props.label}>
          {children.props.children}
        </AccordionItem>
      )}
      {Array.isArray(children) &&
        children.map((item, index) => (
          <AccordionItem value={`item-${index}`} label={item.props.label}>
            {item.props.children}
          </AccordionItem>
        ))}
    </UIAccordion>
  );
};

export default Accordion;
