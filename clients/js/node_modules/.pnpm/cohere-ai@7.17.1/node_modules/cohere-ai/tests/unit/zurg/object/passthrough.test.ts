import { object, string, stringLiteral } from "../../../../src/core/schemas/builders";
import { itJson, itParse, itSchema } from "../utils/itSchema";
import { itValidate } from "../utils/itValidate";

describe("passthrough", () => {
    const baseSchema = object({
        foo: string(),
        bar: stringLiteral("bar"),
    });

    describe("parse", () => {
        itParse("includes unknown values", baseSchema.passthrough(), {
            raw: {
                foo: "hello",
                bar: "bar",
                baz: "extra",
            },
            parsed: {
                foo: "hello",
                bar: "bar",
                baz: "extra",
            },
        });

        itValidate(
            "preserves schema validation",
            baseSchema.passthrough(),
            {
                foo: 123,
                bar: "bar",
                baz: "extra",
            },
            [
                {
                    path: ["foo"],
                    message: "Expected string. Received 123.",
                },
            ]
        );
    });

    describe("json", () => {
        itJson("includes unknown values", baseSchema.passthrough(), {
            raw: {
                foo: "hello",
                bar: "bar",

                baz: "extra",
            },
            parsed: {
                foo: "hello",
                bar: "bar",

                baz: "extra",
            },
        });

        itValidate(
            "preserves schema validation",
            baseSchema.passthrough(),
            {
                foo: "hello",
                bar: "wrong",
                baz: "extra",
            },
            [
                {
                    path: ["bar"],
                    message: 'Expected "bar". Received "wrong".',
                },
            ]
        );
    });

    itSchema("preserves schema validation in both directions", baseSchema.passthrough(), {
        raw: {
            foo: "hello",
            bar: "bar",
            extra: 42,
        },
        parsed: {
            foo: "hello",
            bar: "bar",
            extra: 42,
        },
    });
});
