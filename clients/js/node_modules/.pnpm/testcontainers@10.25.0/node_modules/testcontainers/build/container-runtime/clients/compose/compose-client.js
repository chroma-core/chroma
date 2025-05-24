"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getComposeClient = void 0;
const docker_compose_1 = __importStar(require("docker-compose"));
const common_1 = require("../../../common");
const default_compose_options_1 = require("./default-compose-options");
async function getComposeClient(environment) {
    const info = await getComposeInfo();
    switch (info?.compatability) {
        case undefined:
            return new MissingComposeClient();
        case "v1":
            return new ComposeV1Client(info, environment);
        case "v2":
            return new ComposeV2Client(info, environment);
    }
}
exports.getComposeClient = getComposeClient;
async function getComposeInfo() {
    try {
        return {
            version: (await docker_compose_1.v2.version()).data.version,
            compatability: "v2",
        };
    }
    catch (err) {
        try {
            return {
                version: (await docker_compose_1.default.version()).data.version,
                compatability: "v1",
            };
        }
        catch {
            return undefined;
        }
    }
}
class ComposeV1Client {
    info;
    environment;
    constructor(info, environment) {
        this.info = info;
        this.environment = environment;
    }
    async up(options, services) {
        try {
            if (services) {
                common_1.log.info(`Upping Compose environment services ${services.join(", ")}...`);
                await docker_compose_1.default.upMany(services, await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options));
            }
            else {
                common_1.log.info(`Upping Compose environment...`);
                await docker_compose_1.default.upAll(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options));
            }
            common_1.log.info(`Upped Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => {
                try {
                    common_1.log.error(`Failed to up Compose environment: ${error.message}`);
                    await this.down(options, { removeVolumes: true, timeout: 0 });
                }
                catch {
                    common_1.log.error(`Failed to down Compose environment after failed up`);
                }
            });
        }
    }
    async pull(options, services) {
        try {
            if (services) {
                common_1.log.info(`Pulling Compose environment images "${services.join('", "')}"...`);
                await docker_compose_1.default.pullMany(services, await (0, default_compose_options_1.defaultComposeOptions)(this.environment, { ...options, logger: common_1.pullLog }));
            }
            else {
                common_1.log.info(`Pulling Compose environment images...`);
                await docker_compose_1.default.pullAll(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, { ...options, logger: common_1.pullLog }));
            }
            common_1.log.info(`Pulled Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => common_1.log.error(`Failed to pull Compose environment images: ${error.message}`));
        }
    }
    async stop(options) {
        try {
            common_1.log.info(`Stopping Compose environment...`);
            await docker_compose_1.default.stop(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options));
            common_1.log.info(`Stopped Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => common_1.log.error(`Failed to stop Compose environment: ${error.message}`));
        }
    }
    async down(options, downOptions) {
        try {
            common_1.log.info(`Downing Compose environment...`);
            await docker_compose_1.default.down({
                ...(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options)),
                commandOptions: composeDownCommandOptions(downOptions),
            });
            common_1.log.info(`Downed Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => common_1.log.error(`Failed to down Compose environment: ${error.message}`));
        }
    }
}
class ComposeV2Client {
    info;
    environment;
    constructor(info, environment) {
        this.info = info;
        this.environment = environment;
    }
    async up(options, services) {
        try {
            if (services) {
                common_1.log.info(`Upping Compose environment services ${services.join(", ")}...`);
                await docker_compose_1.v2.upMany(services, await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options));
            }
            else {
                common_1.log.info(`Upping Compose environment...`);
                await docker_compose_1.v2.upAll(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options));
            }
            common_1.log.info(`Upped Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => {
                try {
                    common_1.log.error(`Failed to up Compose environment: ${error.message}`);
                    await this.down(options, { removeVolumes: true, timeout: 0 });
                }
                catch {
                    common_1.log.error(`Failed to down Compose environment after failed up`);
                }
            });
        }
    }
    async pull(options, services) {
        try {
            if (services) {
                common_1.log.info(`Pulling Compose environment images "${services.join('", "')}"...`);
                await docker_compose_1.v2.pullMany(services, await (0, default_compose_options_1.defaultComposeOptions)(this.environment, { ...options, logger: common_1.pullLog }));
            }
            else {
                common_1.log.info(`Pulling Compose environment images...`);
                await docker_compose_1.v2.pullAll(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, { ...options, logger: common_1.pullLog }));
            }
            common_1.log.info(`Pulled Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => common_1.log.error(`Failed to pull Compose environment images: ${error.message}`));
        }
    }
    async stop(options) {
        try {
            common_1.log.info(`Stopping Compose environment...`);
            await docker_compose_1.v2.stop(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options));
            common_1.log.info(`Stopped Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => common_1.log.error(`Failed to stop Compose environment: ${error.message}`));
        }
    }
    async down(options, downOptions) {
        try {
            common_1.log.info(`Downing Compose environment...`);
            await docker_compose_1.v2.down({
                ...(await (0, default_compose_options_1.defaultComposeOptions)(this.environment, options)),
                commandOptions: composeDownCommandOptions(downOptions),
            });
            common_1.log.info(`Downed Compose environment`);
        }
        catch (err) {
            await handleAndRethrow(err, async (error) => common_1.log.error(`Failed to down Compose environment: ${error.message}`));
        }
    }
}
class MissingComposeClient {
    info = undefined;
    up() {
        throw new Error("Compose is not installed");
    }
    pull() {
        throw new Error("Compose is not installed");
    }
    stop() {
        throw new Error("Compose is not installed");
    }
    down() {
        throw new Error("Compose is not installed");
    }
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function handleAndRethrow(err, handle) {
    const error = err instanceof Error ? err : new Error(err.err.trim());
    await handle(error);
    throw error;
}
function composeDownCommandOptions(options) {
    const result = [];
    if (options.removeVolumes) {
        result.push("-v");
    }
    if (options.timeout) {
        result.push("-t", `${options.timeout / 1000}`);
    }
    return result;
}
//# sourceMappingURL=compose-client.js.map