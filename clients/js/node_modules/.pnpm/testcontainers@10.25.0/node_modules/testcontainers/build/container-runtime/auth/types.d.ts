export type CredentialProviderGetResponse = {
    ServerURL: string;
    Username: string;
    Secret: string;
};
export type CredentialProviderListResponse = {
    [registry: string]: string;
};
export type Auth = {
    auth?: string;
    email?: string;
    username?: string;
    password?: string;
};
export type AuthConfig = {
    username: string;
    password: string;
    registryAddress: string;
    email?: string;
};
export type RegistryConfig = {
    [registryAddress: string]: {
        username: string;
        password: string;
    };
};
export type ContainerRuntimeConfig = {
    credHelpers?: {
        [registry: string]: string;
    };
    credsStore?: string;
    auths?: {
        [registry: string]: Auth;
    };
};
