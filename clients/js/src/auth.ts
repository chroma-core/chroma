import {ApiApi as DefaultApi} from "./generated";

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
    METADATA = "metadata"

}

export interface ClientAuthResponse {
    getAuthInfoType(): AuthInfoType;

    getAuthInfo(): { key: string, value: string };
}


export interface AbstractCredentials<T> {
    getCredentials(): T;
}

export interface ClientAuthProtocolAdapter<T> {
    injectCredentials(injectionContext: T): T;

    getApi(): any;
}


class SecretStr {
    constructor(private readonly secret: string) {
    }

    getSecret(): string {
        return this.secret;
    }
}

const base64Encode = (str: string): string => {
    return Buffer.from(str).toString('base64');
};

class BasicAuthCredentials implements AbstractCredentials<SecretStr> {
    private readonly credentials: SecretStr;

    constructor(_creds: string) {
        this.credentials = new SecretStr(base64Encode(_creds))
    }

    getCredentials(): SecretStr {
        //encode base64
        return this.credentials;
    }
}


class BasicAuthClientAuthResponse implements ClientAuthResponse {
    constructor(private readonly credentials: BasicAuthCredentials) {
    }

    getAuthInfo(): { key: string; value: string } {
        return {key: "Authorization", value: "Basic " + this.credentials.getCredentials().getSecret()};
    }

    getAuthInfoType(): AuthInfoType {
        return AuthInfoType.HEADER;
    }
}

export class BasicAuthCredentialsProvider implements ClientAuthCredentialsProvider<BasicAuthCredentials> {
    private readonly credentials: BasicAuthCredentials;

    /**
     * Creates a new BasicAuthCredentialsProvider. This provider loads credentials from provided text credentials or from the environment variable CHROMA_CLIENT_AUTH_CREDENTIALS.
     * @param _creds - The credentials
     * @throws {Error} If neither credentials provider or text credentials are supplied.
     */

    constructor(_creds: string | undefined) {
        if (_creds === undefined && !process.env.CHROMA_CLIENT_AUTH_CREDENTIALS) throw new Error("Credentials must be supplied via environment variable (CHROMA_CLIENT_AUTH_CREDENTIALS) or passed in as configuration.");
        this.credentials = new BasicAuthCredentials((_creds ?? process.env.CHROMA_CLIENT_AUTH_CREDENTIALS) as string);
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

    constructor(options: { textCredentials: any; credentialsProvider: ClientAuthCredentialsProvider<any> | undefined }) {
        if (!options.credentialsProvider && !options.textCredentials) {
            throw new Error("Either credentials provider or text credentials must be supplied.");
        }
        this.credentialsProvider = options.credentialsProvider || new BasicAuthCredentialsProvider(options.textCredentials);
    }

    authenticate(): ClientAuthResponse {
        return new BasicAuthClientAuthResponse(this.credentialsProvider.getCredentials());
    }
}

export class IsomorphicFetchClientAuthProtocolAdapter implements ClientAuthProtocolAdapter<RequestInit> {
    authProvider: ClientAuthProvider | undefined;
    wrapperApi: DefaultApi | undefined;

    /**
     * Creates a new adapter of IsomorphicFetchClientAuthProtocolAdapter.
     * @param api - The API to wrap.
     * @param authConfiguration - The configuration for the authentication provider.
     */

    constructor(private api: DefaultApi, authConfiguration: AuthOptions) {

        switch (authConfiguration.provider) {
            case "basic":
                this.authProvider = new BasicAuthClientAuthProvider({textCredentials: authConfiguration.credentials, credentialsProvider: authConfiguration.credentialsProvider});
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
            const objMethods = Object.getOwnPropertyNames(currentObj)
                .filter(name => typeof currentObj[name] === 'function' && name !== 'constructor');

            methods = methods.concat(objMethods);
            currentObj = Object.getPrototypeOf(currentObj);
        } while (currentObj);

        return methods;
    }

    wrapMethods(obj: any): any {
        let self = this;
        const methodNames = Object.getOwnPropertyNames(Object.getPrototypeOf(obj))
            .filter(name => typeof obj[name] === 'function' && name !== 'constructor');

        return new Proxy(obj, {
            get(target, prop: string) {
                if (methodNames.includes(prop)) {
                    return new Proxy(target[prop], {
                        apply(fn, thisArg, args) {
                            const modifiedArgs = args.map(arg => {
                                if (arg && typeof arg === 'object' && 'method' in arg) {
                                    return self.injectCredentials(arg as RequestInit);
                                }
                                return arg;
                            });
                            if (Object.keys(modifiedArgs[modifiedArgs.length - 1]).length === 0) {
                                modifiedArgs[modifiedArgs.length - 1] = self.injectCredentials({} as RequestInit);
                            } else {
                                modifiedArgs[modifiedArgs.length - 1] = self.injectCredentials(modifiedArgs[modifiedArgs.length - 1] as RequestInit);
                            }
                            return fn.apply(thisArg, modifiedArgs);
                        }
                    });
                }
                return target[prop];
            }
        });
    }

    injectCredentials(injectionContext: RequestInit): RequestInit {
        const authInfo = this.authProvider?.authenticate().getAuthInfo();
        if (authInfo) {
            const {key, value} = authInfo;
            injectionContext = {
                ...injectionContext,
                headers: {
                    [key]: value
                },
            }
        }
        return injectionContext;
    }
}


export type AuthOptions = {
    provider: ClientAuthProvider | string | undefined,
    credentialsProvider?: ClientAuthCredentialsProvider<any> | undefined,
    configProvider?: ClientAuthConfigurationProvider<any> | undefined,
    credentials?: any | undefined,
}
