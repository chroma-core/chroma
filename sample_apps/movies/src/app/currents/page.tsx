import CurrentsBrowser from "@/components/currents-browser";
import currentsPayload from "@/data/foundation-currents.json";

export const metadata = {
  title: "Foundation Currents",
  description: "Browse deduction-first Foundation Currents and their tile entrypoints.",
};

export default function CurrentsPage() {
  return <CurrentsBrowser payload={currentsPayload} />;
}
