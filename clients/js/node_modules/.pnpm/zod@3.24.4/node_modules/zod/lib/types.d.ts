import { enumUtil } from "./helpers/enumUtil";
import { errorUtil } from "./helpers/errorUtil";
import { AsyncParseReturnType, INVALID, ParseContext, ParseInput, ParseParams, ParseReturnType, ParseStatus, SyncParseReturnType } from "./helpers/parseUtil";
import { partialUtil } from "./helpers/partialUtil";
import { Primitive } from "./helpers/typeAliases";
import { objectUtil, util } from "./helpers/util";
import type { StandardSchemaV1 } from "./standard-schema";
import { IssueData, StringValidation, ZodCustomIssue, ZodError, ZodErrorMap } from "./ZodError";
export interface RefinementCtx {
    addIssue: (arg: IssueData) => void;
    path: (string | number)[];
}
export type ZodRawShape = {
    [k: string]: ZodTypeAny;
};
export type ZodTypeAny = ZodType<any, any, any>;
export type TypeOf<T extends ZodType<any, any, any>> = T["_output"];
export type input<T extends ZodType<any, any, any>> = T["_input"];
export type output<T extends ZodType<any, any, any>> = T["_output"];
export type { TypeOf as infer };
export type CustomErrorParams = Partial<util.Omit<ZodCustomIssue, "code">>;
export interface ZodTypeDef {
    errorMap?: ZodErrorMap;
    description?: string;
}
export type RawCreateParams = {
    errorMap?: ZodErrorMap;
    invalid_type_error?: string;
    required_error?: string;
    message?: string;
    description?: string;
} | undefined;
export type ProcessedCreateParams = {
    errorMap?: ZodErrorMap;
    description?: string;
};
export type SafeParseSuccess<Output> = {
    success: true;
    data: Output;
    error?: never;
};
export type SafeParseError<Input> = {
    success: false;
    error: ZodError<Input>;
    data?: never;
};
export type SafeParseReturnType<Input, Output> = SafeParseSuccess<Output> | SafeParseError<Input>;
export declare abstract class ZodType<Output = any, Def extends ZodTypeDef = ZodTypeDef, Input = Output> {
    readonly _type: Output;
    readonly _output: Output;
    readonly _input: Input;
    readonly _def: Def;
    get description(): string | undefined;
    "~standard": StandardSchemaV1.Props<Input, Output>;
    abstract _parse(input: ParseInput): ParseReturnType<Output>;
    _getType(input: ParseInput): string;
    _getOrReturnCtx(input: ParseInput, ctx?: ParseContext | undefined): ParseContext;
    _processInputParams(input: ParseInput): {
        status: ParseStatus;
        ctx: ParseContext;
    };
    _parseSync(input: ParseInput): SyncParseReturnType<Output>;
    _parseAsync(input: ParseInput): AsyncParseReturnType<Output>;
    parse(data: unknown, params?: Partial<ParseParams>): Output;
    safeParse(data: unknown, params?: Partial<ParseParams>): SafeParseReturnType<Input, Output>;
    "~validate"(data: unknown): StandardSchemaV1.Result<Output> | Promise<StandardSchemaV1.Result<Output>>;
    parseAsync(data: unknown, params?: Partial<ParseParams>): Promise<Output>;
    safeParseAsync(data: unknown, params?: Partial<ParseParams>): Promise<SafeParseReturnType<Input, Output>>;
    /** Alias of safeParseAsync */
    spa: (data: unknown, params?: Partial<ParseParams>) => Promise<SafeParseReturnType<Input, Output>>;
    refine<RefinedOutput extends Output>(check: (arg: Output) => arg is RefinedOutput, message?: string | CustomErrorParams | ((arg: Output) => CustomErrorParams)): ZodEffects<this, RefinedOutput, Input>;
    refine(check: (arg: Output) => unknown | Promise<unknown>, message?: string | CustomErrorParams | ((arg: Output) => CustomErrorParams)): ZodEffects<this, Output, Input>;
    refinement<RefinedOutput extends Output>(check: (arg: Output) => arg is RefinedOutput, refinementData: IssueData | ((arg: Output, ctx: RefinementCtx) => IssueData)): ZodEffects<this, RefinedOutput, Input>;
    refinement(check: (arg: Output) => boolean, refinementData: IssueData | ((arg: Output, ctx: RefinementCtx) => IssueData)): ZodEffects<this, Output, Input>;
    _refinement(refinement: RefinementEffect<Output>["refinement"]): ZodEffects<this, Output, Input>;
    superRefine<RefinedOutput extends Output>(refinement: (arg: Output, ctx: RefinementCtx) => arg is RefinedOutput): ZodEffects<this, RefinedOutput, Input>;
    superRefine(refinement: (arg: Output, ctx: RefinementCtx) => void): ZodEffects<this, Output, Input>;
    superRefine(refinement: (arg: Output, ctx: RefinementCtx) => Promise<void>): ZodEffects<this, Output, Input>;
    constructor(def: Def);
    optional(): ZodOptional<this>;
    nullable(): ZodNullable<this>;
    nullish(): ZodOptional<ZodNullable<this>>;
    array(): ZodArray<this>;
    promise(): ZodPromise<this>;
    or<T extends ZodTypeAny>(option: T): ZodUnion<[this, T]>;
    and<T extends ZodTypeAny>(incoming: T): ZodIntersection<this, T>;
    transform<NewOut>(transform: (arg: Output, ctx: RefinementCtx) => NewOut | Promise<NewOut>): ZodEffects<this, NewOut>;
    default(def: util.noUndefined<Input>): ZodDefault<this>;
    default(def: () => util.noUndefined<Input>): ZodDefault<this>;
    brand<B extends string | number | symbol>(brand?: B): ZodBranded<this, B>;
    catch(def: Output): ZodCatch<this>;
    catch(def: (ctx: {
        error: ZodError;
        input: Input;
    }) => Output): ZodCatch<this>;
    describe(description: string): this;
    pipe<T extends ZodTypeAny>(target: T): ZodPipeline<this, T>;
    readonly(): ZodReadonly<this>;
    isOptional(): boolean;
    isNullable(): boolean;
}
export type IpVersion = "v4" | "v6";
export type ZodStringCheck = {
    kind: "min";
    value: number;
    message?: string;
} | {
    kind: "max";
    value: number;
    message?: string;
} | {
    kind: "length";
    value: number;
    message?: string;
} | {
    kind: "email";
    message?: string;
} | {
    kind: "url";
    message?: string;
} | {
    kind: "emoji";
    message?: string;
} | {
    kind: "uuid";
    message?: string;
} | {
    kind: "nanoid";
    message?: string;
} | {
    kind: "cuid";
    message?: string;
} | {
    kind: "includes";
    value: string;
    position?: number;
    message?: string;
} | {
    kind: "cuid2";
    message?: string;
} | {
    kind: "ulid";
    message?: string;
} | {
    kind: "startsWith";
    value: string;
    message?: string;
} | {
    kind: "endsWith";
    value: string;
    message?: string;
} | {
    kind: "regex";
    regex: RegExp;
    message?: string;
} | {
    kind: "trim";
    message?: string;
} | {
    kind: "toLowerCase";
    message?: string;
} | {
    kind: "toUpperCase";
    message?: string;
} | {
    kind: "jwt";
    alg?: string;
    message?: string;
} | {
    kind: "datetime";
    offset: boolean;
    local: boolean;
    precision: number | null;
    message?: string;
} | {
    kind: "date";
    message?: string;
} | {
    kind: "time";
    precision: number | null;
    message?: string;
} | {
    kind: "duration";
    message?: string;
} | {
    kind: "ip";
    version?: IpVersion;
    message?: string;
} | {
    kind: "cidr";
    version?: IpVersion;
    message?: string;
} | {
    kind: "base64";
    message?: string;
} | {
    kind: "base64url";
    message?: string;
};
export interface ZodStringDef extends ZodTypeDef {
    checks: ZodStringCheck[];
    typeName: ZodFirstPartyTypeKind.ZodString;
    coerce: boolean;
}
export declare function datetimeRegex(args: {
    precision?: number | null;
    offset?: boolean;
    local?: boolean;
}): RegExp;
export declare class ZodString extends ZodType<string, ZodStringDef, string> {
    _parse(input: ParseInput): ParseReturnType<string>;
    protected _regex(regex: RegExp, validation: StringValidation, message?: errorUtil.ErrMessage): ZodEffects<this, string, string>;
    _addCheck(check: ZodStringCheck): ZodString;
    email(message?: errorUtil.ErrMessage): ZodString;
    url(message?: errorUtil.ErrMessage): ZodString;
    emoji(message?: errorUtil.ErrMessage): ZodString;
    uuid(message?: errorUtil.ErrMessage): ZodString;
    nanoid(message?: errorUtil.ErrMessage): ZodString;
    cuid(message?: errorUtil.ErrMessage): ZodString;
    cuid2(message?: errorUtil.ErrMessage): ZodString;
    ulid(message?: errorUtil.ErrMessage): ZodString;
    base64(message?: errorUtil.ErrMessage): ZodString;
    base64url(message?: errorUtil.ErrMessage): ZodString;
    jwt(options?: {
        alg?: string;
        message?: string;
    }): ZodString;
    ip(options?: string | {
        version?: IpVersion;
        message?: string;
    }): ZodString;
    cidr(options?: string | {
        version?: IpVersion;
        message?: string;
    }): ZodString;
    datetime(options?: string | {
        message?: string | undefined;
        precision?: number | null;
        offset?: boolean;
        local?: boolean;
    }): ZodString;
    date(message?: string): ZodString;
    time(options?: string | {
        message?: string | undefined;
        precision?: number | null;
    }): ZodString;
    duration(message?: errorUtil.ErrMessage): ZodString;
    regex(regex: RegExp, message?: errorUtil.ErrMessage): ZodString;
    includes(value: string, options?: {
        message?: string;
        position?: number;
    }): ZodString;
    startsWith(value: string, message?: errorUtil.ErrMessage): ZodString;
    endsWith(value: string, message?: errorUtil.ErrMessage): ZodString;
    min(minLength: number, message?: errorUtil.ErrMessage): ZodString;
    max(maxLength: number, message?: errorUtil.ErrMessage): ZodString;
    length(len: number, message?: errorUtil.ErrMessage): ZodString;
    /**
     * Equivalent to `.min(1)`
     */
    nonempty(message?: errorUtil.ErrMessage): ZodString;
    trim(): ZodString;
    toLowerCase(): ZodString;
    toUpperCase(): ZodString;
    get isDatetime(): boolean;
    get isDate(): boolean;
    get isTime(): boolean;
    get isDuration(): boolean;
    get isEmail(): boolean;
    get isURL(): boolean;
    get isEmoji(): boolean;
    get isUUID(): boolean;
    get isNANOID(): boolean;
    get isCUID(): boolean;
    get isCUID2(): boolean;
    get isULID(): boolean;
    get isIP(): boolean;
    get isCIDR(): boolean;
    get isBase64(): boolean;
    get isBase64url(): boolean;
    get minLength(): number | null;
    get maxLength(): number | null;
    static create: (params?: RawCreateParams & {
        coerce?: true;
    }) => ZodString;
}
export type ZodNumberCheck = {
    kind: "min";
    value: number;
    inclusive: boolean;
    message?: string;
} | {
    kind: "max";
    value: number;
    inclusive: boolean;
    message?: string;
} | {
    kind: "int";
    message?: string;
} | {
    kind: "multipleOf";
    value: number;
    message?: string;
} | {
    kind: "finite";
    message?: string;
};
export interface ZodNumberDef extends ZodTypeDef {
    checks: ZodNumberCheck[];
    typeName: ZodFirstPartyTypeKind.ZodNumber;
    coerce: boolean;
}
export declare class ZodNumber extends ZodType<number, ZodNumberDef, number> {
    _parse(input: ParseInput): ParseReturnType<number>;
    static create: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodNumber;
    gte(value: number, message?: errorUtil.ErrMessage): ZodNumber;
    min: (value: number, message?: errorUtil.ErrMessage) => ZodNumber;
    gt(value: number, message?: errorUtil.ErrMessage): ZodNumber;
    lte(value: number, message?: errorUtil.ErrMessage): ZodNumber;
    max: (value: number, message?: errorUtil.ErrMessage) => ZodNumber;
    lt(value: number, message?: errorUtil.ErrMessage): ZodNumber;
    protected setLimit(kind: "min" | "max", value: number, inclusive: boolean, message?: string): ZodNumber;
    _addCheck(check: ZodNumberCheck): ZodNumber;
    int(message?: errorUtil.ErrMessage): ZodNumber;
    positive(message?: errorUtil.ErrMessage): ZodNumber;
    negative(message?: errorUtil.ErrMessage): ZodNumber;
    nonpositive(message?: errorUtil.ErrMessage): ZodNumber;
    nonnegative(message?: errorUtil.ErrMessage): ZodNumber;
    multipleOf(value: number, message?: errorUtil.ErrMessage): ZodNumber;
    step: (value: number, message?: errorUtil.ErrMessage) => ZodNumber;
    finite(message?: errorUtil.ErrMessage): ZodNumber;
    safe(message?: errorUtil.ErrMessage): ZodNumber;
    get minValue(): number | null;
    get maxValue(): number | null;
    get isInt(): boolean;
    get isFinite(): boolean;
}
export type ZodBigIntCheck = {
    kind: "min";
    value: bigint;
    inclusive: boolean;
    message?: string;
} | {
    kind: "max";
    value: bigint;
    inclusive: boolean;
    message?: string;
} | {
    kind: "multipleOf";
    value: bigint;
    message?: string;
};
export interface ZodBigIntDef extends ZodTypeDef {
    checks: ZodBigIntCheck[];
    typeName: ZodFirstPartyTypeKind.ZodBigInt;
    coerce: boolean;
}
export declare class ZodBigInt extends ZodType<bigint, ZodBigIntDef, bigint> {
    _parse(input: ParseInput): ParseReturnType<bigint>;
    _getInvalidInput(input: ParseInput): INVALID;
    static create: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodBigInt;
    gte(value: bigint, message?: errorUtil.ErrMessage): ZodBigInt;
    min: (value: bigint, message?: errorUtil.ErrMessage) => ZodBigInt;
    gt(value: bigint, message?: errorUtil.ErrMessage): ZodBigInt;
    lte(value: bigint, message?: errorUtil.ErrMessage): ZodBigInt;
    max: (value: bigint, message?: errorUtil.ErrMessage) => ZodBigInt;
    lt(value: bigint, message?: errorUtil.ErrMessage): ZodBigInt;
    protected setLimit(kind: "min" | "max", value: bigint, inclusive: boolean, message?: string): ZodBigInt;
    _addCheck(check: ZodBigIntCheck): ZodBigInt;
    positive(message?: errorUtil.ErrMessage): ZodBigInt;
    negative(message?: errorUtil.ErrMessage): ZodBigInt;
    nonpositive(message?: errorUtil.ErrMessage): ZodBigInt;
    nonnegative(message?: errorUtil.ErrMessage): ZodBigInt;
    multipleOf(value: bigint, message?: errorUtil.ErrMessage): ZodBigInt;
    get minValue(): bigint | null;
    get maxValue(): bigint | null;
}
export interface ZodBooleanDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodBoolean;
    coerce: boolean;
}
export declare class ZodBoolean extends ZodType<boolean, ZodBooleanDef, boolean> {
    _parse(input: ParseInput): ParseReturnType<boolean>;
    static create: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodBoolean;
}
export type ZodDateCheck = {
    kind: "min";
    value: number;
    message?: string;
} | {
    kind: "max";
    value: number;
    message?: string;
};
export interface ZodDateDef extends ZodTypeDef {
    checks: ZodDateCheck[];
    coerce: boolean;
    typeName: ZodFirstPartyTypeKind.ZodDate;
}
export declare class ZodDate extends ZodType<Date, ZodDateDef, Date> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    _addCheck(check: ZodDateCheck): ZodDate;
    min(minDate: Date, message?: errorUtil.ErrMessage): ZodDate;
    max(maxDate: Date, message?: errorUtil.ErrMessage): ZodDate;
    get minDate(): Date | null;
    get maxDate(): Date | null;
    static create: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodDate;
}
export interface ZodSymbolDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodSymbol;
}
export declare class ZodSymbol extends ZodType<symbol, ZodSymbolDef, symbol> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: (params?: RawCreateParams) => ZodSymbol;
}
export interface ZodUndefinedDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodUndefined;
}
export declare class ZodUndefined extends ZodType<undefined, ZodUndefinedDef, undefined> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    params?: RawCreateParams;
    static create: (params?: RawCreateParams) => ZodUndefined;
}
export interface ZodNullDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodNull;
}
export declare class ZodNull extends ZodType<null, ZodNullDef, null> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: (params?: RawCreateParams) => ZodNull;
}
export interface ZodAnyDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodAny;
}
export declare class ZodAny extends ZodType<any, ZodAnyDef, any> {
    _any: true;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: (params?: RawCreateParams) => ZodAny;
}
export interface ZodUnknownDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodUnknown;
}
export declare class ZodUnknown extends ZodType<unknown, ZodUnknownDef, unknown> {
    _unknown: true;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: (params?: RawCreateParams) => ZodUnknown;
}
export interface ZodNeverDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodNever;
}
export declare class ZodNever extends ZodType<never, ZodNeverDef, never> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: (params?: RawCreateParams) => ZodNever;
}
export interface ZodVoidDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodVoid;
}
export declare class ZodVoid extends ZodType<void, ZodVoidDef, void> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: (params?: RawCreateParams) => ZodVoid;
}
export interface ZodArrayDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    type: T;
    typeName: ZodFirstPartyTypeKind.ZodArray;
    exactLength: {
        value: number;
        message?: string;
    } | null;
    minLength: {
        value: number;
        message?: string;
    } | null;
    maxLength: {
        value: number;
        message?: string;
    } | null;
}
export type ArrayCardinality = "many" | "atleastone";
export type arrayOutputType<T extends ZodTypeAny, Cardinality extends ArrayCardinality = "many"> = Cardinality extends "atleastone" ? [T["_output"], ...T["_output"][]] : T["_output"][];
export declare class ZodArray<T extends ZodTypeAny, Cardinality extends ArrayCardinality = "many"> extends ZodType<arrayOutputType<T, Cardinality>, ZodArrayDef<T>, Cardinality extends "atleastone" ? [T["_input"], ...T["_input"][]] : T["_input"][]> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get element(): T;
    min(minLength: number, message?: errorUtil.ErrMessage): this;
    max(maxLength: number, message?: errorUtil.ErrMessage): this;
    length(len: number, message?: errorUtil.ErrMessage): this;
    nonempty(message?: errorUtil.ErrMessage): ZodArray<T, "atleastone">;
    static create: <T_1 extends ZodTypeAny>(schema: T_1, params?: RawCreateParams) => ZodArray<T_1, "many">;
}
export type ZodNonEmptyArray<T extends ZodTypeAny> = ZodArray<T, "atleastone">;
export type UnknownKeysParam = "passthrough" | "strict" | "strip";
export interface ZodObjectDef<T extends ZodRawShape = ZodRawShape, UnknownKeys extends UnknownKeysParam = UnknownKeysParam, Catchall extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodObject;
    shape: () => T;
    catchall: Catchall;
    unknownKeys: UnknownKeys;
}
export type mergeTypes<A, B> = {
    [k in keyof A | keyof B]: k extends keyof B ? B[k] : k extends keyof A ? A[k] : never;
};
export type objectOutputType<Shape extends ZodRawShape, Catchall extends ZodTypeAny, UnknownKeys extends UnknownKeysParam = UnknownKeysParam> = objectUtil.flatten<objectUtil.addQuestionMarks<baseObjectOutputType<Shape>>> & CatchallOutput<Catchall> & PassthroughType<UnknownKeys>;
export type baseObjectOutputType<Shape extends ZodRawShape> = {
    [k in keyof Shape]: Shape[k]["_output"];
};
export type objectInputType<Shape extends ZodRawShape, Catchall extends ZodTypeAny, UnknownKeys extends UnknownKeysParam = UnknownKeysParam> = objectUtil.flatten<baseObjectInputType<Shape>> & CatchallInput<Catchall> & PassthroughType<UnknownKeys>;
export type baseObjectInputType<Shape extends ZodRawShape> = objectUtil.addQuestionMarks<{
    [k in keyof Shape]: Shape[k]["_input"];
}>;
export type CatchallOutput<T extends ZodType> = ZodType extends T ? unknown : {
    [k: string]: T["_output"];
};
export type CatchallInput<T extends ZodType> = ZodType extends T ? unknown : {
    [k: string]: T["_input"];
};
export type PassthroughType<T extends UnknownKeysParam> = T extends "passthrough" ? {
    [k: string]: unknown;
} : unknown;
export type deoptional<T extends ZodTypeAny> = T extends ZodOptional<infer U> ? deoptional<U> : T extends ZodNullable<infer U> ? ZodNullable<deoptional<U>> : T;
export type SomeZodObject = ZodObject<ZodRawShape, UnknownKeysParam, ZodTypeAny>;
export type noUnrecognized<Obj extends object, Shape extends object> = {
    [k in keyof Obj]: k extends keyof Shape ? Obj[k] : never;
};
export declare class ZodObject<T extends ZodRawShape, UnknownKeys extends UnknownKeysParam = UnknownKeysParam, Catchall extends ZodTypeAny = ZodTypeAny, Output = objectOutputType<T, Catchall, UnknownKeys>, Input = objectInputType<T, Catchall, UnknownKeys>> extends ZodType<Output, ZodObjectDef<T, UnknownKeys, Catchall>, Input> {
    private _cached;
    _getCached(): {
        shape: T;
        keys: string[];
    };
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get shape(): T;
    strict(message?: errorUtil.ErrMessage): ZodObject<T, "strict", Catchall>;
    strip(): ZodObject<T, "strip", Catchall>;
    passthrough(): ZodObject<T, "passthrough", Catchall>;
    /**
     * @deprecated In most cases, this is no longer needed - unknown properties are now silently stripped.
     * If you want to pass through unknown properties, use `.passthrough()` instead.
     */
    nonstrict: () => ZodObject<T, "passthrough", Catchall>;
    extend<Augmentation extends ZodRawShape>(augmentation: Augmentation): ZodObject<objectUtil.extendShape<T, Augmentation>, UnknownKeys, Catchall>;
    /**
     * @deprecated Use `.extend` instead
     *  */
    augment: <Augmentation extends ZodRawShape>(augmentation: Augmentation) => ZodObject<objectUtil.extendShape<T, Augmentation>, UnknownKeys, Catchall>;
    /**
     * Prior to zod@1.0.12 there was a bug in the
     * inferred type of merged objects. Please
     * upgrade if you are experiencing issues.
     */
    merge<Incoming extends AnyZodObject, Augmentation extends Incoming["shape"]>(merging: Incoming): ZodObject<objectUtil.extendShape<T, Augmentation>, Incoming["_def"]["unknownKeys"], Incoming["_def"]["catchall"]>;
    setKey<Key extends string, Schema extends ZodTypeAny>(key: Key, schema: Schema): ZodObject<T & {
        [k in Key]: Schema;
    }, UnknownKeys, Catchall>;
    catchall<Index extends ZodTypeAny>(index: Index): ZodObject<T, UnknownKeys, Index>;
    pick<Mask extends util.Exactly<{
        [k in keyof T]?: true;
    }, Mask>>(mask: Mask): ZodObject<Pick<T, Extract<keyof T, keyof Mask>>, UnknownKeys, Catchall>;
    omit<Mask extends util.Exactly<{
        [k in keyof T]?: true;
    }, Mask>>(mask: Mask): ZodObject<Omit<T, keyof Mask>, UnknownKeys, Catchall>;
    /**
     * @deprecated
     */
    deepPartial(): partialUtil.DeepPartial<this>;
    partial(): ZodObject<{
        [k in keyof T]: ZodOptional<T[k]>;
    }, UnknownKeys, Catchall>;
    partial<Mask extends util.Exactly<{
        [k in keyof T]?: true;
    }, Mask>>(mask: Mask): ZodObject<objectUtil.noNever<{
        [k in keyof T]: k extends keyof Mask ? ZodOptional<T[k]> : T[k];
    }>, UnknownKeys, Catchall>;
    required(): ZodObject<{
        [k in keyof T]: deoptional<T[k]>;
    }, UnknownKeys, Catchall>;
    required<Mask extends util.Exactly<{
        [k in keyof T]?: true;
    }, Mask>>(mask: Mask): ZodObject<objectUtil.noNever<{
        [k in keyof T]: k extends keyof Mask ? deoptional<T[k]> : T[k];
    }>, UnknownKeys, Catchall>;
    keyof(): ZodEnum<enumUtil.UnionToTupleString<keyof T>>;
    static create: <T_1 extends ZodRawShape>(shape: T_1, params?: RawCreateParams) => ZodObject<T_1, "strip", ZodTypeAny, objectUtil.addQuestionMarks<baseObjectOutputType<T_1>, any> extends infer T_2 ? { [k in keyof T_2]: objectUtil.addQuestionMarks<baseObjectOutputType<T_1>, any>[k]; } : never, baseObjectInputType<T_1> extends infer T_3 ? { [k_1 in keyof T_3]: baseObjectInputType<T_1>[k_1]; } : never>;
    static strictCreate: <T_1 extends ZodRawShape>(shape: T_1, params?: RawCreateParams) => ZodObject<T_1, "strict", ZodTypeAny, objectUtil.addQuestionMarks<baseObjectOutputType<T_1>, any> extends infer T_2 ? { [k in keyof T_2]: objectUtil.addQuestionMarks<baseObjectOutputType<T_1>, any>[k]; } : never, baseObjectInputType<T_1> extends infer T_3 ? { [k_1 in keyof T_3]: baseObjectInputType<T_1>[k_1]; } : never>;
    static lazycreate: <T_1 extends ZodRawShape>(shape: () => T_1, params?: RawCreateParams) => ZodObject<T_1, "strip", ZodTypeAny, objectUtil.addQuestionMarks<baseObjectOutputType<T_1>, any> extends infer T_2 ? { [k in keyof T_2]: objectUtil.addQuestionMarks<baseObjectOutputType<T_1>, any>[k]; } : never, baseObjectInputType<T_1> extends infer T_3 ? { [k_1 in keyof T_3]: baseObjectInputType<T_1>[k_1]; } : never>;
}
export type AnyZodObject = ZodObject<any, any, any>;
export type ZodUnionOptions = Readonly<[ZodTypeAny, ...ZodTypeAny[]]>;
export interface ZodUnionDef<T extends ZodUnionOptions = Readonly<[
    ZodTypeAny,
    ZodTypeAny,
    ...ZodTypeAny[]
]>> extends ZodTypeDef {
    options: T;
    typeName: ZodFirstPartyTypeKind.ZodUnion;
}
export declare class ZodUnion<T extends ZodUnionOptions> extends ZodType<T[number]["_output"], ZodUnionDef<T>, T[number]["_input"]> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get options(): T;
    static create: <T_1 extends readonly [ZodTypeAny, ZodTypeAny, ...ZodTypeAny[]]>(types: T_1, params?: RawCreateParams) => ZodUnion<T_1>;
}
export type ZodDiscriminatedUnionOption<Discriminator extends string> = ZodObject<{
    [key in Discriminator]: ZodTypeAny;
} & ZodRawShape, UnknownKeysParam, ZodTypeAny>;
export interface ZodDiscriminatedUnionDef<Discriminator extends string, Options extends readonly ZodDiscriminatedUnionOption<string>[] = ZodDiscriminatedUnionOption<string>[]> extends ZodTypeDef {
    discriminator: Discriminator;
    options: Options;
    optionsMap: Map<Primitive, ZodDiscriminatedUnionOption<any>>;
    typeName: ZodFirstPartyTypeKind.ZodDiscriminatedUnion;
}
export declare class ZodDiscriminatedUnion<Discriminator extends string, Options extends readonly ZodDiscriminatedUnionOption<Discriminator>[]> extends ZodType<output<Options[number]>, ZodDiscriminatedUnionDef<Discriminator, Options>, input<Options[number]>> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get discriminator(): Discriminator;
    get options(): Options;
    get optionsMap(): Map<Primitive, ZodDiscriminatedUnionOption<any>>;
    /**
     * The constructor of the discriminated union schema. Its behaviour is very similar to that of the normal z.union() constructor.
     * However, it only allows a union of objects, all of which need to share a discriminator property. This property must
     * have a different value for each object in the union.
     * @param discriminator the name of the discriminator property
     * @param types an array of object schemas
     * @param params
     */
    static create<Discriminator extends string, Types extends readonly [
        ZodDiscriminatedUnionOption<Discriminator>,
        ...ZodDiscriminatedUnionOption<Discriminator>[]
    ]>(discriminator: Discriminator, options: Types, params?: RawCreateParams): ZodDiscriminatedUnion<Discriminator, Types>;
}
export interface ZodIntersectionDef<T extends ZodTypeAny = ZodTypeAny, U extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    left: T;
    right: U;
    typeName: ZodFirstPartyTypeKind.ZodIntersection;
}
export declare class ZodIntersection<T extends ZodTypeAny, U extends ZodTypeAny> extends ZodType<T["_output"] & U["_output"], ZodIntersectionDef<T, U>, T["_input"] & U["_input"]> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: <T_1 extends ZodTypeAny, U_1 extends ZodTypeAny>(left: T_1, right: U_1, params?: RawCreateParams) => ZodIntersection<T_1, U_1>;
}
export type ZodTupleItems = [ZodTypeAny, ...ZodTypeAny[]];
export type AssertArray<T> = T extends any[] ? T : never;
export type OutputTypeOfTuple<T extends ZodTupleItems | []> = AssertArray<{
    [k in keyof T]: T[k] extends ZodType<any, any, any> ? T[k]["_output"] : never;
}>;
export type OutputTypeOfTupleWithRest<T extends ZodTupleItems | [], Rest extends ZodTypeAny | null = null> = Rest extends ZodTypeAny ? [...OutputTypeOfTuple<T>, ...Rest["_output"][]] : OutputTypeOfTuple<T>;
export type InputTypeOfTuple<T extends ZodTupleItems | []> = AssertArray<{
    [k in keyof T]: T[k] extends ZodType<any, any, any> ? T[k]["_input"] : never;
}>;
export type InputTypeOfTupleWithRest<T extends ZodTupleItems | [], Rest extends ZodTypeAny | null = null> = Rest extends ZodTypeAny ? [...InputTypeOfTuple<T>, ...Rest["_input"][]] : InputTypeOfTuple<T>;
export interface ZodTupleDef<T extends ZodTupleItems | [] = ZodTupleItems, Rest extends ZodTypeAny | null = null> extends ZodTypeDef {
    items: T;
    rest: Rest;
    typeName: ZodFirstPartyTypeKind.ZodTuple;
}
export type AnyZodTuple = ZodTuple<[
    ZodTypeAny,
    ...ZodTypeAny[]
] | [], ZodTypeAny | null>;
export declare class ZodTuple<T extends [ZodTypeAny, ...ZodTypeAny[]] | [] = [ZodTypeAny, ...ZodTypeAny[]], Rest extends ZodTypeAny | null = null> extends ZodType<OutputTypeOfTupleWithRest<T, Rest>, ZodTupleDef<T, Rest>, InputTypeOfTupleWithRest<T, Rest>> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get items(): T;
    rest<Rest extends ZodTypeAny>(rest: Rest): ZodTuple<T, Rest>;
    static create: <T_1 extends [] | [ZodTypeAny, ...ZodTypeAny[]]>(schemas: T_1, params?: RawCreateParams) => ZodTuple<T_1, null>;
}
export interface ZodRecordDef<Key extends KeySchema = ZodString, Value extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    valueType: Value;
    keyType: Key;
    typeName: ZodFirstPartyTypeKind.ZodRecord;
}
export type KeySchema = ZodType<string | number | symbol, any, any>;
export type RecordType<K extends string | number | symbol, V> = [
    string
] extends [K] ? Record<K, V> : [number] extends [K] ? Record<K, V> : [symbol] extends [K] ? Record<K, V> : [BRAND<string | number | symbol>] extends [K] ? Record<K, V> : Partial<Record<K, V>>;
export declare class ZodRecord<Key extends KeySchema = ZodString, Value extends ZodTypeAny = ZodTypeAny> extends ZodType<RecordType<Key["_output"], Value["_output"]>, ZodRecordDef<Key, Value>, RecordType<Key["_input"], Value["_input"]>> {
    get keySchema(): Key;
    get valueSchema(): Value;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get element(): Value;
    static create<Value extends ZodTypeAny>(valueType: Value, params?: RawCreateParams): ZodRecord<ZodString, Value>;
    static create<Keys extends KeySchema, Value extends ZodTypeAny>(keySchema: Keys, valueType: Value, params?: RawCreateParams): ZodRecord<Keys, Value>;
}
export interface ZodMapDef<Key extends ZodTypeAny = ZodTypeAny, Value extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    valueType: Value;
    keyType: Key;
    typeName: ZodFirstPartyTypeKind.ZodMap;
}
export declare class ZodMap<Key extends ZodTypeAny = ZodTypeAny, Value extends ZodTypeAny = ZodTypeAny> extends ZodType<Map<Key["_output"], Value["_output"]>, ZodMapDef<Key, Value>, Map<Key["_input"], Value["_input"]>> {
    get keySchema(): Key;
    get valueSchema(): Value;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: <Key_1 extends ZodTypeAny = ZodTypeAny, Value_1 extends ZodTypeAny = ZodTypeAny>(keyType: Key_1, valueType: Value_1, params?: RawCreateParams) => ZodMap<Key_1, Value_1>;
}
export interface ZodSetDef<Value extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    valueType: Value;
    typeName: ZodFirstPartyTypeKind.ZodSet;
    minSize: {
        value: number;
        message?: string;
    } | null;
    maxSize: {
        value: number;
        message?: string;
    } | null;
}
export declare class ZodSet<Value extends ZodTypeAny = ZodTypeAny> extends ZodType<Set<Value["_output"]>, ZodSetDef<Value>, Set<Value["_input"]>> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    min(minSize: number, message?: errorUtil.ErrMessage): this;
    max(maxSize: number, message?: errorUtil.ErrMessage): this;
    size(size: number, message?: errorUtil.ErrMessage): this;
    nonempty(message?: errorUtil.ErrMessage): ZodSet<Value>;
    static create: <Value_1 extends ZodTypeAny = ZodTypeAny>(valueType: Value_1, params?: RawCreateParams) => ZodSet<Value_1>;
}
export interface ZodFunctionDef<Args extends ZodTuple<any, any> = ZodTuple<any, any>, Returns extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    args: Args;
    returns: Returns;
    typeName: ZodFirstPartyTypeKind.ZodFunction;
}
export type OuterTypeOfFunction<Args extends ZodTuple<any, any>, Returns extends ZodTypeAny> = Args["_input"] extends Array<any> ? (...args: Args["_input"]) => Returns["_output"] : never;
export type InnerTypeOfFunction<Args extends ZodTuple<any, any>, Returns extends ZodTypeAny> = Args["_output"] extends Array<any> ? (...args: Args["_output"]) => Returns["_input"] : never;
export declare class ZodFunction<Args extends ZodTuple<any, any>, Returns extends ZodTypeAny> extends ZodType<OuterTypeOfFunction<Args, Returns>, ZodFunctionDef<Args, Returns>, InnerTypeOfFunction<Args, Returns>> {
    _parse(input: ParseInput): ParseReturnType<any>;
    parameters(): Args;
    returnType(): Returns;
    args<Items extends Parameters<(typeof ZodTuple)["create"]>[0]>(...items: Items): ZodFunction<ZodTuple<Items, ZodUnknown>, Returns>;
    returns<NewReturnType extends ZodType<any, any, any>>(returnType: NewReturnType): ZodFunction<Args, NewReturnType>;
    implement<F extends InnerTypeOfFunction<Args, Returns>>(func: F): ReturnType<F> extends Returns["_output"] ? (...args: Args["_input"]) => ReturnType<F> : OuterTypeOfFunction<Args, Returns>;
    strictImplement(func: InnerTypeOfFunction<Args, Returns>): InnerTypeOfFunction<Args, Returns>;
    validate: <F extends InnerTypeOfFunction<Args, Returns>>(func: F) => ReturnType<F> extends Returns["_output"] ? (...args: Args["_input"]) => ReturnType<F> : OuterTypeOfFunction<Args, Returns>;
    static create(): ZodFunction<ZodTuple<[], ZodUnknown>, ZodUnknown>;
    static create<T extends AnyZodTuple = ZodTuple<[], ZodUnknown>>(args: T): ZodFunction<T, ZodUnknown>;
    static create<T extends AnyZodTuple, U extends ZodTypeAny>(args: T, returns: U): ZodFunction<T, U>;
    static create<T extends AnyZodTuple = ZodTuple<[], ZodUnknown>, U extends ZodTypeAny = ZodUnknown>(args: T, returns: U, params?: RawCreateParams): ZodFunction<T, U>;
}
export interface ZodLazyDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    getter: () => T;
    typeName: ZodFirstPartyTypeKind.ZodLazy;
}
export declare class ZodLazy<T extends ZodTypeAny> extends ZodType<output<T>, ZodLazyDef<T>, input<T>> {
    get schema(): T;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: <T_1 extends ZodTypeAny>(getter: () => T_1, params?: RawCreateParams) => ZodLazy<T_1>;
}
export interface ZodLiteralDef<T = any> extends ZodTypeDef {
    value: T;
    typeName: ZodFirstPartyTypeKind.ZodLiteral;
}
export declare class ZodLiteral<T> extends ZodType<T, ZodLiteralDef<T>, T> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get value(): T;
    static create: <T_1 extends Primitive>(value: T_1, params?: RawCreateParams) => ZodLiteral<T_1>;
}
export type ArrayKeys = keyof any[];
export type Indices<T> = Exclude<keyof T, ArrayKeys>;
export type EnumValues<T extends string = string> = readonly [T, ...T[]];
export type Values<T extends EnumValues> = {
    [k in T[number]]: k;
};
export interface ZodEnumDef<T extends EnumValues = EnumValues> extends ZodTypeDef {
    values: T;
    typeName: ZodFirstPartyTypeKind.ZodEnum;
}
export type Writeable<T> = {
    -readonly [P in keyof T]: T[P];
};
export type FilterEnum<Values, ToExclude> = Values extends [] ? [] : Values extends [infer Head, ...infer Rest] ? Head extends ToExclude ? FilterEnum<Rest, ToExclude> : [Head, ...FilterEnum<Rest, ToExclude>] : never;
export type typecast<A, T> = A extends T ? A : never;
declare function createZodEnum<U extends string, T extends Readonly<[U, ...U[]]>>(values: T, params?: RawCreateParams): ZodEnum<Writeable<T>>;
declare function createZodEnum<U extends string, T extends [U, ...U[]]>(values: T, params?: RawCreateParams): ZodEnum<T>;
export declare class ZodEnum<T extends [string, ...string[]]> extends ZodType<T[number], ZodEnumDef<T>, T[number]> {
    #private;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    get options(): T;
    get enum(): Values<T>;
    get Values(): Values<T>;
    get Enum(): Values<T>;
    extract<ToExtract extends readonly [T[number], ...T[number][]]>(values: ToExtract, newDef?: RawCreateParams): ZodEnum<Writeable<ToExtract>>;
    exclude<ToExclude extends readonly [T[number], ...T[number][]]>(values: ToExclude, newDef?: RawCreateParams): ZodEnum<typecast<Writeable<FilterEnum<T, ToExclude[number]>>, [string, ...string[]]>>;
    static create: typeof createZodEnum;
}
export interface ZodNativeEnumDef<T extends EnumLike = EnumLike> extends ZodTypeDef {
    values: T;
    typeName: ZodFirstPartyTypeKind.ZodNativeEnum;
}
export type EnumLike = {
    [k: string]: string | number;
    [nu: number]: string;
};
export declare class ZodNativeEnum<T extends EnumLike> extends ZodType<T[keyof T], ZodNativeEnumDef<T>, T[keyof T]> {
    #private;
    _parse(input: ParseInput): ParseReturnType<T[keyof T]>;
    get enum(): T;
    static create: <T_1 extends EnumLike>(values: T_1, params?: RawCreateParams) => ZodNativeEnum<T_1>;
}
export interface ZodPromiseDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    type: T;
    typeName: ZodFirstPartyTypeKind.ZodPromise;
}
export declare class ZodPromise<T extends ZodTypeAny> extends ZodType<Promise<T["_output"]>, ZodPromiseDef<T>, Promise<T["_input"]>> {
    unwrap(): T;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: <T_1 extends ZodTypeAny>(schema: T_1, params?: RawCreateParams) => ZodPromise<T_1>;
}
export type Refinement<T> = (arg: T, ctx: RefinementCtx) => any;
export type SuperRefinement<T> = (arg: T, ctx: RefinementCtx) => void | Promise<void>;
export type RefinementEffect<T> = {
    type: "refinement";
    refinement: (arg: T, ctx: RefinementCtx) => any;
};
export type TransformEffect<T> = {
    type: "transform";
    transform: (arg: T, ctx: RefinementCtx) => any;
};
export type PreprocessEffect<T> = {
    type: "preprocess";
    transform: (arg: T, ctx: RefinementCtx) => any;
};
export type Effect<T> = RefinementEffect<T> | TransformEffect<T> | PreprocessEffect<T>;
export interface ZodEffectsDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    schema: T;
    typeName: ZodFirstPartyTypeKind.ZodEffects;
    effect: Effect<any>;
}
export declare class ZodEffects<T extends ZodTypeAny, Output = output<T>, Input = input<T>> extends ZodType<Output, ZodEffectsDef<T>, Input> {
    innerType(): T;
    sourceType(): T;
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: <I extends ZodTypeAny>(schema: I, effect: Effect<I["_output"]>, params?: RawCreateParams) => ZodEffects<I, I["_output"]>;
    static createWithPreprocess: <I extends ZodTypeAny>(preprocess: (arg: unknown, ctx: RefinementCtx) => unknown, schema: I, params?: RawCreateParams) => ZodEffects<I, I["_output"], unknown>;
}
export { ZodEffects as ZodTransformer };
export interface ZodOptionalDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    innerType: T;
    typeName: ZodFirstPartyTypeKind.ZodOptional;
}
export type ZodOptionalType<T extends ZodTypeAny> = ZodOptional<T>;
export declare class ZodOptional<T extends ZodTypeAny> extends ZodType<T["_output"] | undefined, ZodOptionalDef<T>, T["_input"] | undefined> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    unwrap(): T;
    static create: <T_1 extends ZodTypeAny>(type: T_1, params?: RawCreateParams) => ZodOptional<T_1>;
}
export interface ZodNullableDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    innerType: T;
    typeName: ZodFirstPartyTypeKind.ZodNullable;
}
export type ZodNullableType<T extends ZodTypeAny> = ZodNullable<T>;
export declare class ZodNullable<T extends ZodTypeAny> extends ZodType<T["_output"] | null, ZodNullableDef<T>, T["_input"] | null> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    unwrap(): T;
    static create: <T_1 extends ZodTypeAny>(type: T_1, params?: RawCreateParams) => ZodNullable<T_1>;
}
export interface ZodDefaultDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    innerType: T;
    defaultValue: () => util.noUndefined<T["_input"]>;
    typeName: ZodFirstPartyTypeKind.ZodDefault;
}
export declare class ZodDefault<T extends ZodTypeAny> extends ZodType<util.noUndefined<T["_output"]>, ZodDefaultDef<T>, T["_input"] | undefined> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    removeDefault(): T;
    static create: <T_1 extends ZodTypeAny>(type: T_1, params: {
        errorMap?: ZodErrorMap | undefined;
        invalid_type_error?: string | undefined;
        required_error?: string | undefined;
        message?: string | undefined;
        description?: string | undefined;
    } & {
        default: T_1["_input"] | (() => util.noUndefined<T_1["_input"]>);
    }) => ZodDefault<T_1>;
}
export interface ZodCatchDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    innerType: T;
    catchValue: (ctx: {
        error: ZodError;
        input: unknown;
    }) => T["_input"];
    typeName: ZodFirstPartyTypeKind.ZodCatch;
}
export declare class ZodCatch<T extends ZodTypeAny> extends ZodType<T["_output"], ZodCatchDef<T>, unknown> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    removeCatch(): T;
    static create: <T_1 extends ZodTypeAny>(type: T_1, params: {
        errorMap?: ZodErrorMap | undefined;
        invalid_type_error?: string | undefined;
        required_error?: string | undefined;
        message?: string | undefined;
        description?: string | undefined;
    } & {
        catch: T_1["_output"] | (() => T_1["_output"]);
    }) => ZodCatch<T_1>;
}
export interface ZodNaNDef extends ZodTypeDef {
    typeName: ZodFirstPartyTypeKind.ZodNaN;
}
export declare class ZodNaN extends ZodType<number, ZodNaNDef, number> {
    _parse(input: ParseInput): ParseReturnType<any>;
    static create: (params?: RawCreateParams) => ZodNaN;
}
export interface ZodBrandedDef<T extends ZodTypeAny> extends ZodTypeDef {
    type: T;
    typeName: ZodFirstPartyTypeKind.ZodBranded;
}
export declare const BRAND: unique symbol;
export type BRAND<T extends string | number | symbol> = {
    [BRAND]: {
        [k in T]: true;
    };
};
export declare class ZodBranded<T extends ZodTypeAny, B extends string | number | symbol> extends ZodType<T["_output"] & BRAND<B>, ZodBrandedDef<T>, T["_input"]> {
    _parse(input: ParseInput): ParseReturnType<any>;
    unwrap(): T;
}
export interface ZodPipelineDef<A extends ZodTypeAny, B extends ZodTypeAny> extends ZodTypeDef {
    in: A;
    out: B;
    typeName: ZodFirstPartyTypeKind.ZodPipeline;
}
export declare class ZodPipeline<A extends ZodTypeAny, B extends ZodTypeAny> extends ZodType<B["_output"], ZodPipelineDef<A, B>, A["_input"]> {
    _parse(input: ParseInput): ParseReturnType<any>;
    static create<A extends ZodTypeAny, B extends ZodTypeAny>(a: A, b: B): ZodPipeline<A, B>;
}
type BuiltIn = (((...args: any[]) => any) | (new (...args: any[]) => any)) | {
    readonly [Symbol.toStringTag]: string;
} | Date | Error | Generator | Promise<unknown> | RegExp;
type MakeReadonly<T> = T extends Map<infer K, infer V> ? ReadonlyMap<K, V> : T extends Set<infer V> ? ReadonlySet<V> : T extends [infer Head, ...infer Tail] ? readonly [Head, ...Tail] : T extends Array<infer V> ? ReadonlyArray<V> : T extends BuiltIn ? T : Readonly<T>;
export interface ZodReadonlyDef<T extends ZodTypeAny = ZodTypeAny> extends ZodTypeDef {
    innerType: T;
    typeName: ZodFirstPartyTypeKind.ZodReadonly;
}
export declare class ZodReadonly<T extends ZodTypeAny> extends ZodType<MakeReadonly<T["_output"]>, ZodReadonlyDef<T>, MakeReadonly<T["_input"]>> {
    _parse(input: ParseInput): ParseReturnType<this["_output"]>;
    static create: <T_1 extends ZodTypeAny>(type: T_1, params?: RawCreateParams) => ZodReadonly<T_1>;
    unwrap(): T;
}
type CustomParams = CustomErrorParams & {
    fatal?: boolean;
};
export declare function custom<T>(check?: (data: any) => any, _params?: string | CustomParams | ((input: any) => CustomParams), 
/**
 * @deprecated
 *
 * Pass `fatal` into the params object instead:
 *
 * ```ts
 * z.string().custom((val) => val.length > 5, { fatal: false })
 * ```
 *
 */
fatal?: boolean): ZodType<T, ZodTypeDef, T>;
export { ZodType as Schema, ZodType as ZodSchema };
export declare const late: {
    object: <T extends ZodRawShape>(shape: () => T, params?: RawCreateParams) => ZodObject<T, "strip">;
};
export declare enum ZodFirstPartyTypeKind {
    ZodString = "ZodString",
    ZodNumber = "ZodNumber",
    ZodNaN = "ZodNaN",
    ZodBigInt = "ZodBigInt",
    ZodBoolean = "ZodBoolean",
    ZodDate = "ZodDate",
    ZodSymbol = "ZodSymbol",
    ZodUndefined = "ZodUndefined",
    ZodNull = "ZodNull",
    ZodAny = "ZodAny",
    ZodUnknown = "ZodUnknown",
    ZodNever = "ZodNever",
    ZodVoid = "ZodVoid",
    ZodArray = "ZodArray",
    ZodObject = "ZodObject",
    ZodUnion = "ZodUnion",
    ZodDiscriminatedUnion = "ZodDiscriminatedUnion",
    ZodIntersection = "ZodIntersection",
    ZodTuple = "ZodTuple",
    ZodRecord = "ZodRecord",
    ZodMap = "ZodMap",
    ZodSet = "ZodSet",
    ZodFunction = "ZodFunction",
    ZodLazy = "ZodLazy",
    ZodLiteral = "ZodLiteral",
    ZodEnum = "ZodEnum",
    ZodEffects = "ZodEffects",
    ZodNativeEnum = "ZodNativeEnum",
    ZodOptional = "ZodOptional",
    ZodNullable = "ZodNullable",
    ZodDefault = "ZodDefault",
    ZodCatch = "ZodCatch",
    ZodPromise = "ZodPromise",
    ZodBranded = "ZodBranded",
    ZodPipeline = "ZodPipeline",
    ZodReadonly = "ZodReadonly"
}
export type ZodFirstPartySchemaTypes = ZodString | ZodNumber | ZodNaN | ZodBigInt | ZodBoolean | ZodDate | ZodUndefined | ZodNull | ZodAny | ZodUnknown | ZodNever | ZodVoid | ZodArray<any, any> | ZodObject<any, any, any> | ZodUnion<any> | ZodDiscriminatedUnion<any, any> | ZodIntersection<any, any> | ZodTuple<any, any> | ZodRecord<any, any> | ZodMap<any> | ZodSet<any> | ZodFunction<any, any> | ZodLazy<any> | ZodLiteral<any> | ZodEnum<any> | ZodEffects<any, any, any> | ZodNativeEnum<any> | ZodOptional<any> | ZodNullable<any> | ZodDefault<any> | ZodCatch<any> | ZodPromise<any> | ZodBranded<any, any> | ZodPipeline<any, any> | ZodReadonly<any> | ZodSymbol;
declare abstract class Class {
    constructor(..._: any[]);
}
declare const instanceOfType: <T extends typeof Class>(cls: T, params?: CustomParams) => ZodType<InstanceType<T>, ZodTypeDef, InstanceType<T>>;
declare const stringType: (params?: RawCreateParams & {
    coerce?: true;
}) => ZodString;
declare const numberType: (params?: RawCreateParams & {
    coerce?: boolean;
}) => ZodNumber;
declare const nanType: (params?: RawCreateParams) => ZodNaN;
declare const bigIntType: (params?: RawCreateParams & {
    coerce?: boolean;
}) => ZodBigInt;
declare const booleanType: (params?: RawCreateParams & {
    coerce?: boolean;
}) => ZodBoolean;
declare const dateType: (params?: RawCreateParams & {
    coerce?: boolean;
}) => ZodDate;
declare const symbolType: (params?: RawCreateParams) => ZodSymbol;
declare const undefinedType: (params?: RawCreateParams) => ZodUndefined;
declare const nullType: (params?: RawCreateParams) => ZodNull;
declare const anyType: (params?: RawCreateParams) => ZodAny;
declare const unknownType: (params?: RawCreateParams) => ZodUnknown;
declare const neverType: (params?: RawCreateParams) => ZodNever;
declare const voidType: (params?: RawCreateParams) => ZodVoid;
declare const arrayType: <T extends ZodTypeAny>(schema: T, params?: RawCreateParams) => ZodArray<T>;
declare const objectType: <T extends ZodRawShape>(shape: T, params?: RawCreateParams) => ZodObject<T, "strip", ZodTypeAny, objectOutputType<T, ZodTypeAny, "strip">, objectInputType<T, ZodTypeAny, "strip">>;
declare const strictObjectType: <T extends ZodRawShape>(shape: T, params?: RawCreateParams) => ZodObject<T, "strict">;
declare const unionType: <T extends readonly [ZodTypeAny, ZodTypeAny, ...ZodTypeAny[]]>(types: T, params?: RawCreateParams) => ZodUnion<T>;
declare const discriminatedUnionType: typeof ZodDiscriminatedUnion.create;
declare const intersectionType: <T extends ZodTypeAny, U extends ZodTypeAny>(left: T, right: U, params?: RawCreateParams) => ZodIntersection<T, U>;
declare const tupleType: <T extends [] | [ZodTypeAny, ...ZodTypeAny[]]>(schemas: T, params?: RawCreateParams) => ZodTuple<T, null>;
declare const recordType: typeof ZodRecord.create;
declare const mapType: <Key extends ZodTypeAny = ZodTypeAny, Value extends ZodTypeAny = ZodTypeAny>(keyType: Key, valueType: Value, params?: RawCreateParams) => ZodMap<Key, Value>;
declare const setType: <Value extends ZodTypeAny = ZodTypeAny>(valueType: Value, params?: RawCreateParams) => ZodSet<Value>;
declare const functionType: typeof ZodFunction.create;
declare const lazyType: <T extends ZodTypeAny>(getter: () => T, params?: RawCreateParams) => ZodLazy<T>;
declare const literalType: <T extends Primitive>(value: T, params?: RawCreateParams) => ZodLiteral<T>;
declare const enumType: typeof createZodEnum;
declare const nativeEnumType: <T extends EnumLike>(values: T, params?: RawCreateParams) => ZodNativeEnum<T>;
declare const promiseType: <T extends ZodTypeAny>(schema: T, params?: RawCreateParams) => ZodPromise<T>;
declare const effectsType: <I extends ZodTypeAny>(schema: I, effect: Effect<I["_output"]>, params?: RawCreateParams) => ZodEffects<I, I["_output"]>;
declare const optionalType: <T extends ZodTypeAny>(type: T, params?: RawCreateParams) => ZodOptional<T>;
declare const nullableType: <T extends ZodTypeAny>(type: T, params?: RawCreateParams) => ZodNullable<T>;
declare const preprocessType: <I extends ZodTypeAny>(preprocess: (arg: unknown, ctx: RefinementCtx) => unknown, schema: I, params?: RawCreateParams) => ZodEffects<I, I["_output"], unknown>;
declare const pipelineType: typeof ZodPipeline.create;
declare const ostring: () => ZodOptional<ZodString>;
declare const onumber: () => ZodOptional<ZodNumber>;
declare const oboolean: () => ZodOptional<ZodBoolean>;
export declare const coerce: {
    string: (params?: RawCreateParams & {
        coerce?: true;
    }) => ZodString;
    number: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodNumber;
    boolean: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodBoolean;
    bigint: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodBigInt;
    date: (params?: RawCreateParams & {
        coerce?: boolean;
    }) => ZodDate;
};
export { anyType as any, arrayType as array, bigIntType as bigint, booleanType as boolean, dateType as date, discriminatedUnionType as discriminatedUnion, effectsType as effect, enumType as enum, functionType as function, instanceOfType as instanceof, intersectionType as intersection, lazyType as lazy, literalType as literal, mapType as map, nanType as nan, nativeEnumType as nativeEnum, neverType as never, nullType as null, nullableType as nullable, numberType as number, objectType as object, oboolean, onumber, optionalType as optional, ostring, pipelineType as pipeline, preprocessType as preprocess, promiseType as promise, recordType as record, setType as set, strictObjectType as strictObject, stringType as string, symbolType as symbol, effectsType as transformer, tupleType as tuple, undefinedType as undefined, unionType as union, unknownType as unknown, voidType as void, };
export declare const NEVER: never;
