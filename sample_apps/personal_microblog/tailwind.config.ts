import type { Config } from "tailwindcss";

export default {
  content: [
    "./src/pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "var(--background)",
        foreground: "var(--foreground)",
      },
    },
    fontFamily: {
      display: ["monospace", "serif"],
      body: ["system-ui", "-apple-system", "system-ui", "Segoe UI", "Roboto", "sans-serif", "var(--font-gelasio)", "serif"],
      ui: ["system-ui", "-apple-system", "system-ui", "Segoe UI", "Roboto", "sans-serif"],
    },
  },
  plugins: [],
} satisfies Config;
