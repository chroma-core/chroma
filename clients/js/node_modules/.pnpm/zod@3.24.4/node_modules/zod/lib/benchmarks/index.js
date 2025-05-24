"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const datetime_1 = __importDefault(require("./datetime"));
const discriminatedUnion_1 = __importDefault(require("./discriminatedUnion"));
const ipv4_1 = __importDefault(require("./ipv4"));
const object_1 = __importDefault(require("./object"));
const primitives_1 = __importDefault(require("./primitives"));
const realworld_1 = __importDefault(require("./realworld"));
const string_1 = __importDefault(require("./string"));
const union_1 = __importDefault(require("./union"));
const argv = process.argv.slice(2);
let suites = [];
if (!argv.length) {
    suites = [
        ...realworld_1.default.suites,
        ...primitives_1.default.suites,
        ...string_1.default.suites,
        ...object_1.default.suites,
        ...union_1.default.suites,
        ...discriminatedUnion_1.default.suites,
    ];
}
else {
    if (argv.includes("--realworld")) {
        suites.push(...realworld_1.default.suites);
    }
    if (argv.includes("--primitives")) {
        suites.push(...primitives_1.default.suites);
    }
    if (argv.includes("--string")) {
        suites.push(...string_1.default.suites);
    }
    if (argv.includes("--object")) {
        suites.push(...object_1.default.suites);
    }
    if (argv.includes("--union")) {
        suites.push(...union_1.default.suites);
    }
    if (argv.includes("--discriminatedUnion")) {
        suites.push(...datetime_1.default.suites);
    }
    if (argv.includes("--datetime")) {
        suites.push(...datetime_1.default.suites);
    }
    if (argv.includes("--ipv4")) {
        suites.push(...ipv4_1.default.suites);
    }
}
for (const suite of suites) {
    suite.run({});
}
// exit on Ctrl-C
process.on("SIGINT", function () {
    console.log("Exiting...");
    process.exit();
});
