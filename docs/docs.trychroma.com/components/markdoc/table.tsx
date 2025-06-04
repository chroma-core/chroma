import {
  Table as UITable,
  TableHeader as UITableHeader,
  TableBody as UITableBody,
  TableRow as UITableRow,
  TableHead as UITableHead,
  TableCell as UITableCell,
} from "@/components/ui/table";
import React from "react";

export const Table = React.forwardRef<
  HTMLTableElement,
  React.HTMLAttributes<HTMLTableElement>
>(({ ...props }, ref) => {
  return (
    <div className="relative w-full overflow-auto rounded-md my-5 border-[0.5px] border-gray-300 dark:border-gray-700">
      <UITable ref={ref} className="m-0" {...props} />
    </div>
  );
});
Table.displayName = "Table";

export const TableHeader = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ ...props }, ref) => (
  <UITableHeader ref={ref} className="border-none bg-gray-900" {...props} />
));
TableHeader.displayName = "TableHeader";

export const TableBody = React.forwardRef<
  HTMLTableSectionElement,
  React.HTMLAttributes<HTMLTableSectionElement>
>(({ ...props }, ref) => <UITableBody ref={ref} {...props} />);
TableBody.displayName = "TableBody";

export const TableRow = React.forwardRef<
  HTMLTableRowElement,
  React.HTMLAttributes<HTMLTableRowElement>
>(({ ...props }, ref) => <UITableRow ref={ref} className="" {...props} />);
TableRow.displayName = "TableRow";

export const TableHead = React.forwardRef<
  HTMLTableCellElement,
  React.ThHTMLAttributes<HTMLTableCellElement>
>(({ ...props }, ref) => (
  <UITableHead
    ref={ref}
    className="text-gray-200 dark:text-gray-200 py-1"
    {...props}
  />
));
TableHead.displayName = "TableHead";

export const TableCell = React.forwardRef<
  HTMLTableCellElement,
  React.TdHTMLAttributes<HTMLTableCellElement>
>(({ ...props }, ref) => <UITableCell className="" ref={ref} {...props} />);
TableCell.displayName = "TableCell";
