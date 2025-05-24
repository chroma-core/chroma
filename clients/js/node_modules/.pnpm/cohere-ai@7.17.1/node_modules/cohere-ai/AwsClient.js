"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AwsClientV2 = exports.AwsClient = void 0;
const Client_1 = require("./Client");
const ClientV2_1 = require("./ClientV2");
class AwsClient extends Client_1.CohereClient {
    constructor(_options) {
        _options.token = "n/a"; // AWS clients don't need a token but setting to this to a string so Fern doesn't complain
        super(_options);
    }
}
exports.AwsClient = AwsClient;
class AwsClientV2 extends ClientV2_1.CohereClientV2 {
    constructor(_options) {
        _options.token = "n/a"; // AWS clients don't need a token but setting to this to a string so Fern doesn't complain
        super(_options);
    }
}
exports.AwsClientV2 = AwsClientV2;
