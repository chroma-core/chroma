import { LucideIcon } from "lucide-react";

export interface AppSection {
  id: string;
  name: string;
  target: string;
  default: string;
  icon: LucideIcon;
  comingSoon?: boolean;
  subSections: string[];
}

export interface PageMetadata {
  id: string;
  title: string;
  section: string;
  order: number;
}
