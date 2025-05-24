"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.bigint = void 0;
const Schema_1 = require("../../Schema");
const getErrorMessageForIncorrectType_1 = require("../../utils/getErrorMessageForIncorrectType");
const maybeSkipValidation_1 = require("../../utils/maybeSkipValidation");
const schema_utils_1 = require("../schema-utils");
function bigint() {
    const baseSchema = {
        parse: (raw, { breadcrumbsPrefix = [] } = {}) => {
            if (typeof raw !== "string") {
                return {
                    ok: false,
                    errors: [
                        {
                            path: breadcrumbsPrefix,
                            message: (0, getErrorMessageForIncorrectType_1.getErrorMessageForIncorrectType)(raw, "string"),
                        },
                    ],
                };
            }
            return {
                ok: true,
                value: BigInt(raw),
            };
        },
        json: (bigint, { breadcrumbsPrefix = [] } = {}) => {
            if (typeof bigint === "bigint") {
                return {
                    ok: true,
                    value: bigint.toString(),
                };
            }
            else {
                return {
                    ok: false,
                    errors: [
                        {
                            path: breadcrumbsPrefix,
                            message: (0, getErrorMessageForIncorrectType_1.getErrorMessageForIncorrectType)(bigint, "bigint"),
                        },
                    ],
                };
            }
        },
        getType: () => Schema_1.SchemaType.BIGINT,
    };
    return Object.assign(Object.assign({}, (0, maybeSkipValidation_1.maybeSkipValidation)(baseSchema)), (0, schema_utils_1.getSchemaUtils)(baseSchema));
}
exports.bigint = bigint;
