"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CredentialProvider = void 0;
const child_process_1 = require("child_process");
const common_1 = require("../../common");
const registry_matches_1 = require("./registry-matches");
class CredentialProvider {
    async getAuthConfig(registry, dockerConfig) {
        const credentialProviderName = this.getCredentialProviderName(registry, dockerConfig);
        if (!credentialProviderName) {
            return undefined;
        }
        const programName = `docker-credential-${credentialProviderName}`;
        common_1.log.debug(`Executing Docker credential provider "${programName}"`);
        const credentials = await this.listCredentials(programName);
        const credentialForRegistry = Object.keys(credentials).find((aRegistry) => (0, registry_matches_1.registryMatches)(aRegistry, registry));
        if (!credentialForRegistry) {
            common_1.log.debug(`No credential found for registry "${registry}"`);
            return undefined;
        }
        const response = await this.runCredentialProvider(registry, programName);
        return {
            username: response.Username,
            password: response.Secret,
            registryAddress: response.ServerURL ?? credentialForRegistry,
        };
    }
    listCredentials(providerName) {
        return new Promise((resolve, reject) => {
            (0, child_process_1.exec)(`${providerName} list`, (err, stdout, stderr) => {
                if (err) {
                    if (stderr === "list is unimplemented\n") {
                        return resolve({});
                    }
                    common_1.log.error(`An error occurred listing credentials: ${err}`);
                    return reject(new Error("An error occurred listing credentials"));
                }
                try {
                    const response = JSON.parse(stdout);
                    return resolve(response);
                }
                catch (e) {
                    common_1.log.error(`Unexpected response from Docker credential provider LIST command: "${stdout}"`);
                    return reject(new Error("Unexpected response from Docker credential provider LIST command"));
                }
            });
        });
    }
    runCredentialProvider(registry, providerName) {
        return new Promise((resolve, reject) => {
            const sink = (0, child_process_1.spawn)(providerName, ["get"]);
            const chunks = [];
            sink.stdout.on("data", (chunk) => chunks.push(chunk));
            sink.on("close", (code) => {
                if (code !== 0) {
                    common_1.log.error(`An error occurred getting a credential: ${code}`);
                    return reject(new Error("An error occurred getting a credential"));
                }
                const response = chunks.join("");
                try {
                    const parsedResponse = JSON.parse(response);
                    return resolve(parsedResponse);
                }
                catch (e) {
                    common_1.log.error(`Unexpected response from Docker credential provider GET command: "${response}"`);
                    return reject(new Error("Unexpected response from Docker credential provider GET command"));
                }
            });
            sink.stdin.write(`${registry}\n`);
            sink.stdin.end();
        });
    }
}
exports.CredentialProvider = CredentialProvider;
//# sourceMappingURL=credential-provider.js.map