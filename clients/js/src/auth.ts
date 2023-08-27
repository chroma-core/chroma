import {ApiApi as DefaultApi} from "./generated";

export interface ClientAuthProvider {
    authenticate(): ClientAuthResponse;
}

export interface ClientAuthConfigurationProvider<T> {
    getConfig(): T;
}

export interface ClientAuthCredentialsProvider<T> {
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
    private readonly credentials: BasicAuthCredentials

    constructor(_creds: string | undefined) {
        if (_creds === undefined && !process.env.CHROMA_CLIENT_AUTH_CREDENTIALS) throw new Error("Credentials must be supplied via environment variable (CHROMA_CLIENT_AUTH_CREDENTIALS) or passed in as configuration.");
        this.credentials = new BasicAuthCredentials((_creds ?? process.env.CHROMA_CLIENT_AUTH_CREDENTIALS) as string);
    }

    getCredentials(): BasicAuthCredentials {
        return this.credentials;
    }
}

class BasicAuthClientAuthProvider implements ClientAuthProvider {
    constructor(private readonly credentialsProvider: BasicAuthCredentialsProvider) {
    }

    authenticate(): ClientAuthResponse {
        return new BasicAuthClientAuthResponse(this.credentialsProvider.getCredentials());
    }
}

export class IsomorphicFetchClientAuthProtocolAdapter implements ClientAuthProtocolAdapter<RequestInit> {
    authProvider: ClientAuthProvider | undefined;
    wrapperApi: DefaultApi | undefined;

    constructor(private api: DefaultApi, authProvider: string | undefined, creds: string | undefined) {
        switch (authProvider) {
            case "basic":

                this.authProvider = new BasicAuthClientAuthProvider(new BasicAuthCredentialsProvider(creds));
                break;
            default:
                this.authProvider = undefined;
                break;
        }
        if (this.authProvider !== undefined) {
            this.wrapperApi = this.wrapApi();
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
                                // modifiedArgs.push({} as RequestInit);
                                modifiedArgs[modifiedArgs.length - 1] = self.injectCredentials({} as RequestInit);
                            } else {
                                modifiedArgs[modifiedArgs.length - 1] = self.injectCredentials(modifiedArgs[modifiedArgs.length - 1] as RequestInit);
                            }
                            // thisArg.configuration.authorization = "Basic YWRtaW46YWRtaW4K"
                            console.log(`Called ${prop} with args:`, modifiedArgs);
                            return fn.apply(thisArg, modifiedArgs);
                        }
                    });
                }
                return target[prop];
            }
        });
    }

    wrapApi() {
        return this.wrapMethods(this.api);
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
