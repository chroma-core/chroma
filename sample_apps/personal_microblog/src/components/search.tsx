"use client";

import { Command } from "cmdk";
import { useState, useEffect } from "react";

export default function Search() {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      // Check for cmd+k, cmd+p, ctrl+k, or ctrl+p
      if (
        (event.metaKey || event.ctrlKey) &&
        (event.key === 'k' || event.key === 'p')
      ) {
        event.preventDefault();
        setOpen(true);
      }
    };

    document.addEventListener('keydown', handleKeyDown);

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, []);

  if (!open) {
    return null;
  }

  return (
    <SearchBar open={open} setOpen={setOpen} />
  );
}

function SearchBar({ open, setOpen }: { open: boolean, setOpen: (open: boolean) => void }) {
  return (
    <Command.Dialog open={open} onOpenChange={setOpen} label="Global Command Menu">
      <Command.Input />
      <Command.List>
        <Command.Empty>No results found.</Command.Empty>

        <Command.Group heading="Letters">
          <Command.Item>a</Command.Item>
          <Command.Item>b</Command.Item>
          <Command.Separator />
          <Command.Item>c</Command.Item>
        </Command.Group>

        <Command.Item>Apple</Command.Item>
      </Command.List>
    </Command.Dialog>
  );
}
