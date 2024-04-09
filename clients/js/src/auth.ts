import { ApiApi as DefaultApi } from "./generated";

export interface ClientAuthProvider {
  /**
   * Abstract method for authenticating a client.
   */
  authenticate(): ClientAuthResponse;
}

export interface ClientAuthConfigurationProvider<T> {
  /**
   * Abstract method for getting the configuration for the client.
   */
  getConfig(): T;
}

export interface ClientAuthCredentialsProvider<T> {
  /**
   * Abstract method for getting the credentials for the client.
   * @param user
   */
  getCredentials(user?: string): T;
}

enum AuthInfoType {
  COOKIE = "cookie",
  HEADER = "header",
  URL = "url",
  METADATA = "metadata",
}

export interface ClientAuthResponse {
  getAuthInfoType(): AuthInfoType;

  getAuthInfo(): { key: string; value: string };
}

export interface AbstractCredentials<T> {
  getCredentials(): T;
}

export interface ClientAuthProtocolAdapter<T> {
  injectCredentials(injectionContext: T): T;

  getApi(): any;
}

class SecretStr {
  constructor(private readonly secret: string) { }

  getSecret(): string {
    return this.secret;
  }
}

const base64Encode = (str: string): string => {
  return Buffer.from(str).toString("base64");
};

class BasicAuthCredentials implements AbstractCredentials<SecretStr> {
  private readonly credentials: SecretStr;

  constructor(_creds: string) {
    this.credentials = new SecretStr(base64Encode(_creds));
  }

  getCredentials(): SecretStr {
    //encode base64
    return this.credentials;
  }
}

class BasicAuthClientAuthResponse implements ClientAuthResponse {
  constructor(private readonly credentials: BasicAuthCredentials) { }

  getAuthInfo(): { key: string; value: string } {
    return {
      key: "Authorization",
      value: "Basic " + this.credentials.getCredentials().getSecret(),
    };
  }

  getAuthInfoType(): AuthInfoType {
    return AuthInfoType.HEADER;
  }
}

export class BasicAuthCredentialsProvider
  implements ClientAuthCredentialsProvider<BasicAuthCredentials>
{
  private readonly credentials: BasicAuthCredentials;

  /**
   * Creates a new BasicAuthCredentialsProvider. This provider loads credentials from provided text credentials or from the environment variable CHROMA_CLIENT_AUTH_CREDENTIALS.
   * @param _creds - The credentials
   * @throws {Error} If neither credentials provider or text credentials are supplied.
   */

  constructor(_creds: string | undefined) {
    if (_creds === undefined && !process.env.CHROMA_CLIENT_AUTH_CREDENTIALS)
      throw new Error(
        "Credentials must be supplied via environment variable (CHROMA_CLIENT_AUTH_CREDENTIALS) or passed in as configuration.",
      );
    this.credentials = new BasicAuthCredentials(
      (_creds ?? process.env.CHROMA_CLIENT_AUTH_CREDENTIALS) as string,
    );
  }

  getCredentials(): BasicAuthCredentials {
    return this.credentials;
  }
}

class BasicAuthClientAuthProvider implements ClientAuthProvider {
  private readonly credentialsProvider: ClientAuthCredentialsProvider<any>;

  /**
   * Creates a new BasicAuthClientAuthProvider.
   * @param options - The options for the authentication provider.
   * @param options.textCredentials - The credentials for the authentication provider.
   * @param options.credentialsProvider - The credentials provider for the authentication provider.
   * @throws {Error} If neither credentials provider or text credentials are supplied.
   */

  constructor(options: {
    textCredentials: any;
    credentialsProvider: ClientAuthCredentialsProvider<any> | undefined;
  }) {
    if (!options.credentialsProvider && !options.textCredentials) {
      throw new Error(
        "Either credentials provider or text credentials must be supplied.",
      );
    }
    this.credentialsProvider =
      options.credentialsProvider ||
      new BasicAuthCredentialsProvider(options.textCredentials);
  }

  authenticate(): ClientAuthResponse {
    return new BasicAuthClientAuthResponse(
      this.credentialsProvider.getCredentials(),
    );
  }
}

class TokenAuthCredentials implements AbstractCredentials<SecretStr> {
  private readonly credentials: SecretStr;

  constructor(_creds: string) {
    this.credentials = new SecretStr(_creds);
  }

  getCredentials(): SecretStr {
    return this.credentials;
  }
}

export class TokenCredentialsProvider
  implements ClientAuthCredentialsProvider<TokenAuthCredentials>
{
  private readonly credentials: TokenAuthCredentials;

  constructor(_creds: string | undefined) {
    if (_creds === undefined && !process.env.CHROMA_CLIENT_AUTH_CREDENTIALS)
      throw new Error(
        "Credentials must be supplied via environment variable (CHROMA_CLIENT_AUTH_CREDENTIALS) or passed in as configuration.",
      );
    this.credentials = new TokenAuthCredentials(
      (_creds ?? process.env.CHROMA_CLIENT_AUTH_CREDENTIALS) as string,
    );
  }

  getCredentials(): TokenAuthCredentials {
    return this.credentials;
  }
}

export class TokenClientAuthProvider implements ClientAuthProvider {
  private readonly credentialsProvider: ClientAuthCredentialsProvider<any>;
  private readonly providerOptions: { headerType: TokenHeaderType };

  constructor(options: {
    textCredentials: any;
    credentialsProvider: ClientAuthCredentialsProvider<any> | undefined;
    providerOptions?: { headerType: TokenHeaderType };
  }) {
    if (!options.credentialsProvider && !options.textCredentials) {
      throw new Error(
        "Either credentials provider or text credentials must be supplied.",
      );
    }
    if (
      options.providerOptions === undefined ||
      !options.providerOptions.hasOwnProperty("headerType")
    ) {
      this.providerOptions = { headerType: "AUTHORIZATION" };
    } else {
      this.providerOptions = { headerType: options.providerOptions.headerType };
    }
    this.credentialsProvider =
      options.credentialsProvider ||
      new TokenCredentialsProvider(options.textCredentials);
  }

  authenticate(): ClientAuthResponse {
    return new TokenClientAuthResponse(
      this.credentialsProvider.getCredentials(),
      this.providerOptions.headerType,
    );
  }
}

type TokenHeaderType = "AUTHORIZATION" | "X_CHROMA_TOKEN";

const TokenHeader: Record<
  TokenHeaderType,
  (value: string) => { key: string; value: string }
> = {
  AUTHORIZATION: (value: string) => ({
    key: "Authorization",
    value: `Bearer ${value}`,
  }),
  X_CHROMA_TOKEN: (value: string) => ({ key: "X-Chroma-Token", value: value }),
};

class TokenClientAuthResponse implements ClientAuthResponse {
  constructor(
    private readonly credentials: TokenAuthCredentials,
    private readonly headerType: TokenHeaderType = "AUTHORIZATION",
  ) { }

  getAuthInfo(): { key: string; value: string } {
    if (this.headerType === "AUTHORIZATION") {
      return TokenHeader.AUTHORIZATION(
        this.credentials.getCredentials().getSecret(),
      );
    } else if (this.headerType === "X_CHROMA_TOKEN") {
      return TokenHeader.X_CHROMA_TOKEN(
        this.credentials.getCredentials().getSecret(),
      );
    } else {
      throw new Error(
        "Invalid header type: " +
        this.headerType +
        ". Valid types are: " +
        Object.keys(TokenHeader).join(", "),
      );
    }
  }

  getAuthInfoType(): AuthInfoType {
    return AuthInfoType.HEADER;
  }
}

export class IsomorphicFetchClientAuthProtocolAdapter
  implements ClientAuthProtocolAdapter<RequestInit>
{
  authProvider: ClientAuthProvider | undefined;
  wrapperApi: DefaultApi | undefined;

  /**
   * Creates a new adapter of IsomorphicFetchClientAuthProtocolAdapter.
   * @param api - The API to wrap.
   * @param authConfiguration - The configuration for the authentication provider.
   */

  constructor(
    private api: DefaultApi,
    authConfiguration: AuthOptions,
  ) {
    switch (authConfiguration.provider) {
      case "basic":
        this.authProvider = new BasicAuthClientAuthProvider({
          textCredentials: authConfiguration.credentials,
          credentialsProvider: authConfiguration.credentialsProvider,
        });
        break;
      case "token":
        this.authProvider = new TokenClientAuthProvider({
          textCredentials: authConfiguration.credentials,
          credentialsProvider: authConfiguration.credentialsProvider,
          providerOptions: authConfiguration.providerOptions,
        });
        break;
      default:
        this.authProvider = undefined;
        break;
    }
    if (this.authProvider !== undefined) {
      this.wrapperApi = this.wrapMethods(this.api);
    }
  }

  getApi(): DefaultApi {
    return this.wrapperApi ?? this.api;
  }

  getAllMethods(obj: any): string[] {
    let methods: string[] = [];
    let currentObj = obj;

    do {
      const objMethods = Object.getOwnPropertyNames(currentObj).filter(
        (name) =>
          typeof currentObj[name] === "function" && name !== "constructor",
      );

      methods = methods.concat(objMethods);
      currentObj = Object.getPrototypeOf(currentObj);
    } while (currentObj);

    return methods;
  }

  wrapMethods(obj: any): any {
    let self = this;
    const methodNames = Object.getOwnPropertyNames(
      Object.getPrototypeOf(obj),
    ).filter(
      (name) => typeof obj[name] === "function" && name !== "constructor",
    );

    return new Proxy(obj, {
      get(target, prop: string) {
        if (methodNames.includes(prop)) {
          return new Proxy(target[prop], {
            apply(fn, thisArg, args) {
              const modifiedArgs = args.map((arg) => {
                if (arg && typeof arg === "object" && "method" in arg) {
                  return self.injectCredentials(arg as RequestInit);
                }
                return arg;
              });
              if (
                Object.keys(modifiedArgs[modifiedArgs.length - 1]).length === 0
              ) {
                modifiedArgs[modifiedArgs.length - 1] = self.injectCredentials(
                  {} as RequestInit,
                );
              } else {
                modifiedArgs[modifiedArgs.length - 1] = self.injectCredentials(
                  modifiedArgs[modifiedArgs.length - 1] as RequestInit,
                );
              }
              return fn.apply(thisArg, modifiedArgs);
            },
          });
        }
        return target[prop];
      },
    });
  }

  injectCredentials(injectionContext: RequestInit): RequestInit {
    const authInfo = this.authProvider?.authenticate().getAuthInfo();
    if (authInfo) {
      const { key, value } = authInfo;
      injectionContext = {
        ...injectionContext,
        headers: {
          [key]: value,
        },
      };
    }
    return injectionContext;
  }
}

export type AuthOptions = {
  provider: ClientAuthProvider | string | undefined;
  credentialsProvider?: ClientAuthCredentialsProvider<any> | undefined;
  configProvider?: ClientAuthConfigurationProvider<any> | undefined;
  credentials?: any | undefined;
  providerOptions?: any | undefined;
};