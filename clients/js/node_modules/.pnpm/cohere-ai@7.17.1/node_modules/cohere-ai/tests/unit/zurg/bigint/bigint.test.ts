import { bigint } from "../../../../src/core/schemas/builders/bigint";
import { itSchema } from "../utils/itSchema";
import { itValidateJson, itValidateParse } from "../utils/itValidate";

describe("bigint", () => {
    itSchema("converts between raw string and parsed bigint", bigint(), {
        raw: "123456789012345678901234567890123456789012345678901234567890",
        parsed: BigInt("123456789012345678901234567890123456789012345678901234567890"),
    });

    itValidateParse("non-string", bigint(), 42, [
        {
            message: "Expected string. Received 42.",
            path: [],
        },
    ]);

    itValidateJson("non-bigint", bigint(), "hello", [
        {
            message: 'Expected bigint. Received "hello".',
            path: [],
        },
    ]);
});
