export type AuthHeaders = { [header: string]: string };
type TokenHeaderType = "AUTHORIZATION" | "X_CHROMA_TOKEN";

export type AuthOptions = {
  provider: ClientAuthProvider | string | undefined;
  credentials?: any | undefined;

  // Only relevant for token auth
  tokenHeaderType?: TokenHeaderType | undefined;
};

export const tokenHeaderTypeToHeaderKey = (
  headerType: TokenHeaderType,
): string => {
  if (headerType === "AUTHORIZATION") {
    return "Authorization";
  } else {
    return "X-Chroma-Token";
  }
};

const base64Encode = (str: string): string => {
  return Buffer.from(str).toString("base64");
};

export interface ClientAuthProvider {
  /**
   * Abstract method for authenticating a client.
   */
  authenticate(): AuthHeaders;
}

export class BasicAuthClientProvider implements ClientAuthProvider {
  private readonly credentials: AuthHeaders;

  /**
   * Creates a new BasicAuthClientProvider.
   * @param textCredentials - The credentials for the authentication provider. Must be of the form "username:password". If not supplied, the environment variable CHROMA_CLIENT_AUTH_CREDENTIALS will be used.
   * @throws {Error} If neither credentials provider or text credentials are supplied.
   */
  constructor(textCredentials: string | undefined) {
    const creds = textCredentials ?? process.env.CHROMA_CLIENT_AUTH_CREDENTIALS;
    if (creds === undefined) {
      throw new Error(
        "Credentials must be supplied via environment variable (CHROMA_CLIENT_AUTH_CREDENTIALS) or passed in as configuration.",
      );
    }
    this.credentials = {
      Authorization: "Basic " + base64Encode(creds),
    };
  }

  authenticate(): AuthHeaders {
    return this.credentials;
  }
}

export class TokenAuthClientProvider implements ClientAuthProvider {
  private readonly credentials: AuthHeaders;

  constructor(
    textCredentials: any,
    headerType: TokenHeaderType = "AUTHORIZATION",
  ) {
    const creds = textCredentials ?? process.env.CHROMA_CLIENT_AUTH_CREDENTIALS;
    if (creds === undefined) {
      throw new Error(
        "Credentials must be supplied via environment variable (CHROMA_CLIENT_AUTH_CREDENTIALS) or passed in as configuration.",
      );
    }

    const headerKey: string = tokenHeaderTypeToHeaderKey(headerType);
    const headerVal =
      headerType === "AUTHORIZATION" ? `Bearer ${creds}` : creds;
    this.credentials = {};
    this.credentials[headerKey] = headerVal;
  }

  authenticate(): AuthHeaders {
    return this.credentials;
  }
}

export const authOptionsToAuthProvider = (
  auth: AuthOptions,
): ClientAuthProvider => {
  if (auth.provider === undefined) {
    throw new Error("Auth provider not specified");
  }
  if (auth.credentials === undefined) {
    throw new Error("Auth credentials not specified");
  }
  switch (auth.provider) {
    case "basic":
      return new BasicAuthClientProvider(auth.credentials);
    case "token":
      return new TokenAuthClientProvider(
        auth.credentials,
        auth.tokenHeaderType,
      );
      break;
    default:
      throw new Error("Invalid auth provider");
  }
};
