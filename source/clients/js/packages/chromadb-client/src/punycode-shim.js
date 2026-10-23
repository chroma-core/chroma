// This shim provides punycode functionality to prevent dynamic require errors
import punycode from "punycode";

// Add to globalThis to make it available for dynamic requires
globalThis.punycode = punycode;

// Export punycode directly
export default punycode;
