import { GoogleTagManager } from "@next/third-parties/google";

export default function GTM() {
  const gtmId = process.env.NEXT_PUBLIC_GTM_MEASUREMENT_ID;

  if (!gtmId) return null;

  return <GoogleTagManager gtmId={gtmId} />;
}
