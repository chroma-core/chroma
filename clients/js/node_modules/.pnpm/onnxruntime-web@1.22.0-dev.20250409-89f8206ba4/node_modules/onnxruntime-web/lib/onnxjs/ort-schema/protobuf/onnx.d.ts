import Long from 'long';
import * as $protobuf from 'protobufjs';

/** Namespace onnx. */
export namespace onnx {

  /** Version enum. */
  enum Version {
    _START_VERSION = 0,
    IR_VERSION_2017_10_10 = 1,
    IR_VERSION_2017_10_30 = 2,
    IR_VERSION_2017_11_3 = 3,
    IR_VERSION_2019_1_22 = 4,
    IR_VERSION_2019_3_18 = 5,
    IR_VERSION_2019_9_19 = 6,
    IR_VERSION_2020_5_8 = 7,
    IR_VERSION_2021_7_30 = 8,
    IR_VERSION = 9
  }

  /** Properties of an AttributeProto. */
  interface IAttributeProto {
    /** AttributeProto name */
    name?: (string|null);

    /** AttributeProto refAttrName */
    refAttrName?: (string|null);

    /** AttributeProto docString */
    docString?: (string|null);

    /** AttributeProto type */
    type?: (onnx.AttributeProto.AttributeType|null);

    /** AttributeProto f */
    f?: (number|null);

    /** AttributeProto i */
    i?: (number|Long|null);

    /** AttributeProto s */
    s?: (Uint8Array|null);

    /** AttributeProto t */
    t?: (onnx.ITensorProto|null);

    /** AttributeProto g */
    g?: (onnx.IGraphProto|null);

    /** AttributeProto sparseTensor */
    sparseTensor?: (onnx.ISparseTensorProto|null);

    /** AttributeProto tp */
    tp?: (onnx.ITypeProto|null);

    /** AttributeProto floats */
    floats?: (number[]|null);

    /** AttributeProto ints */
    ints?: ((number | Long)[]|null);

    /** AttributeProto strings */
    strings?: (Uint8Array[]|null);

    /** AttributeProto tensors */
    tensors?: (onnx.ITensorProto[]|null);

    /** AttributeProto graphs */
    graphs?: (onnx.IGraphProto[]|null);

    /** AttributeProto sparseTensors */
    sparseTensors?: (onnx.ISparseTensorProto[]|null);

    /** AttributeProto typeProtos */
    typeProtos?: (onnx.ITypeProto[]|null);
  }

  /** Represents an AttributeProto. */
  class AttributeProto implements IAttributeProto {
    /**
     * Constructs a new AttributeProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IAttributeProto);

    /** AttributeProto name. */
    public name: string;

    /** AttributeProto refAttrName. */
    public refAttrName: string;

    /** AttributeProto docString. */
    public docString: string;

    /** AttributeProto type. */
    public type: onnx.AttributeProto.AttributeType;

    /** AttributeProto f. */
    public f: number;

    /** AttributeProto i. */
    public i: (number|Long);

    /** AttributeProto s. */
    public s: Uint8Array;

    /** AttributeProto t. */
    public t?: (onnx.ITensorProto|null);

    /** AttributeProto g. */
    public g?: (onnx.IGraphProto|null);

    /** AttributeProto sparseTensor. */
    public sparseTensor?: (onnx.ISparseTensorProto|null);

    /** AttributeProto tp. */
    public tp?: (onnx.ITypeProto|null);

    /** AttributeProto floats. */
    public floats: number[];

    /** AttributeProto ints. */
    public ints: (number|Long)[];

    /** AttributeProto strings. */
    public strings: Uint8Array[];

    /** AttributeProto tensors. */
    public tensors: onnx.ITensorProto[];

    /** AttributeProto graphs. */
    public graphs: onnx.IGraphProto[];

    /** AttributeProto sparseTensors. */
    public sparseTensors: onnx.ISparseTensorProto[];

    /** AttributeProto typeProtos. */
    public typeProtos: onnx.ITypeProto[];

    /**
     * Creates a new AttributeProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns AttributeProto instance
     */
    public static create(properties?: onnx.IAttributeProto): onnx.AttributeProto;

    /**
     * Encodes the specified AttributeProto message. Does not implicitly {@link onnx.AttributeProto.verify|verify}
     * messages.
     * @param message AttributeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IAttributeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified AttributeProto message, length delimited. Does not implicitly {@link
     * onnx.AttributeProto.verify|verify} messages.
     * @param message AttributeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IAttributeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an AttributeProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns AttributeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.AttributeProto;

    /**
     * Decodes an AttributeProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns AttributeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.AttributeProto;

    /**
     * Verifies an AttributeProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates an AttributeProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns AttributeProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.AttributeProto;

    /**
     * Creates a plain object from an AttributeProto message. Also converts values to other types if specified.
     * @param message AttributeProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.AttributeProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this AttributeProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for AttributeProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  namespace AttributeProto {

    /** AttributeType enum. */
    enum AttributeType {
      UNDEFINED = 0,
      FLOAT = 1,
      INT = 2,
      STRING = 3,
      TENSOR = 4,
      GRAPH = 5,
      SPARSE_TENSOR = 11,
      TYPE_PROTO = 13,
      FLOATS = 6,
      INTS = 7,
      STRINGS = 8,
      TENSORS = 9,
      GRAPHS = 10,
      SPARSE_TENSORS = 12,
      TYPE_PROTOS = 14
    }
  }

  /** Properties of a ValueInfoProto. */
  interface IValueInfoProto {
    /** ValueInfoProto name */
    name?: (string|null);

    /** ValueInfoProto type */
    type?: (onnx.ITypeProto|null);

    /** ValueInfoProto docString */
    docString?: (string|null);
  }

  /** Represents a ValueInfoProto. */
  class ValueInfoProto implements IValueInfoProto {
    /**
     * Constructs a new ValueInfoProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IValueInfoProto);

    /** ValueInfoProto name. */
    public name: string;

    /** ValueInfoProto type. */
    public type?: (onnx.ITypeProto|null);

    /** ValueInfoProto docString. */
    public docString: string;

    /**
     * Creates a new ValueInfoProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ValueInfoProto instance
     */
    public static create(properties?: onnx.IValueInfoProto): onnx.ValueInfoProto;

    /**
     * Encodes the specified ValueInfoProto message. Does not implicitly {@link onnx.ValueInfoProto.verify|verify}
     * messages.
     * @param message ValueInfoProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IValueInfoProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ValueInfoProto message, length delimited. Does not implicitly {@link
     * onnx.ValueInfoProto.verify|verify} messages.
     * @param message ValueInfoProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IValueInfoProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a ValueInfoProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ValueInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.ValueInfoProto;

    /**
     * Decodes a ValueInfoProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ValueInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.ValueInfoProto;

    /**
     * Verifies a ValueInfoProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a ValueInfoProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ValueInfoProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.ValueInfoProto;

    /**
     * Creates a plain object from a ValueInfoProto message. Also converts values to other types if specified.
     * @param message ValueInfoProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.ValueInfoProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this ValueInfoProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for ValueInfoProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a NodeProto. */
  interface INodeProto {
    /** NodeProto input */
    input?: (string[]|null);

    /** NodeProto output */
    output?: (string[]|null);

    /** NodeProto name */
    name?: (string|null);

    /** NodeProto opType */
    opType?: (string|null);

    /** NodeProto domain */
    domain?: (string|null);

    /** NodeProto attribute */
    attribute?: (onnx.IAttributeProto[]|null);

    /** NodeProto docString */
    docString?: (string|null);
  }

  /** Represents a NodeProto. */
  class NodeProto implements INodeProto {
    /**
     * Constructs a new NodeProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.INodeProto);

    /** NodeProto input. */
    public input: string[];

    /** NodeProto output. */
    public output: string[];

    /** NodeProto name. */
    public name: string;

    /** NodeProto opType. */
    public opType: string;

    /** NodeProto domain. */
    public domain: string;

    /** NodeProto attribute. */
    public attribute: onnx.IAttributeProto[];

    /** NodeProto docString. */
    public docString: string;

    /**
     * Creates a new NodeProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns NodeProto instance
     */
    public static create(properties?: onnx.INodeProto): onnx.NodeProto;

    /**
     * Encodes the specified NodeProto message. Does not implicitly {@link onnx.NodeProto.verify|verify} messages.
     * @param message NodeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.INodeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified NodeProto message, length delimited. Does not implicitly {@link
     * onnx.NodeProto.verify|verify} messages.
     * @param message NodeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.INodeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a NodeProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns NodeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.NodeProto;

    /**
     * Decodes a NodeProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns NodeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.NodeProto;

    /**
     * Verifies a NodeProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a NodeProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns NodeProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.NodeProto;

    /**
     * Creates a plain object from a NodeProto message. Also converts values to other types if specified.
     * @param message NodeProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.NodeProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this NodeProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for NodeProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a TrainingInfoProto. */
  interface ITrainingInfoProto {
    /** TrainingInfoProto initialization */
    initialization?: (onnx.IGraphProto|null);

    /** TrainingInfoProto algorithm */
    algorithm?: (onnx.IGraphProto|null);

    /** TrainingInfoProto initializationBinding */
    initializationBinding?: (onnx.IStringStringEntryProto[]|null);

    /** TrainingInfoProto updateBinding */
    updateBinding?: (onnx.IStringStringEntryProto[]|null);
  }

  /** Represents a TrainingInfoProto. */
  class TrainingInfoProto implements ITrainingInfoProto {
    /**
     * Constructs a new TrainingInfoProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.ITrainingInfoProto);

    /** TrainingInfoProto initialization. */
    public initialization?: (onnx.IGraphProto|null);

    /** TrainingInfoProto algorithm. */
    public algorithm?: (onnx.IGraphProto|null);

    /** TrainingInfoProto initializationBinding. */
    public initializationBinding: onnx.IStringStringEntryProto[];

    /** TrainingInfoProto updateBinding. */
    public updateBinding: onnx.IStringStringEntryProto[];

    /**
     * Creates a new TrainingInfoProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns TrainingInfoProto instance
     */
    public static create(properties?: onnx.ITrainingInfoProto): onnx.TrainingInfoProto;

    /**
     * Encodes the specified TrainingInfoProto message. Does not implicitly {@link onnx.TrainingInfoProto.verify|verify}
     * messages.
     * @param message TrainingInfoProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.ITrainingInfoProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified TrainingInfoProto message, length delimited. Does not implicitly {@link
     * onnx.TrainingInfoProto.verify|verify} messages.
     * @param message TrainingInfoProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.ITrainingInfoProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a TrainingInfoProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns TrainingInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TrainingInfoProto;

    /**
     * Decodes a TrainingInfoProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns TrainingInfoProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TrainingInfoProto;

    /**
     * Verifies a TrainingInfoProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a TrainingInfoProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns TrainingInfoProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.TrainingInfoProto;

    /**
     * Creates a plain object from a TrainingInfoProto message. Also converts values to other types if specified.
     * @param message TrainingInfoProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.TrainingInfoProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this TrainingInfoProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for TrainingInfoProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a ModelProto. */
  interface IModelProto {
    /** ModelProto irVersion */
    irVersion?: (number|Long|null);

    /** ModelProto opsetImport */
    opsetImport?: (onnx.IOperatorSetIdProto[]|null);

    /** ModelProto producerName */
    producerName?: (string|null);

    /** ModelProto producerVersion */
    producerVersion?: (string|null);

    /** ModelProto domain */
    domain?: (string|null);

    /** ModelProto modelVersion */
    modelVersion?: (number|Long|null);

    /** ModelProto docString */
    docString?: (string|null);

    /** ModelProto graph */
    graph?: (onnx.IGraphProto|null);

    /** ModelProto metadataProps */
    metadataProps?: (onnx.IStringStringEntryProto[]|null);

    /** ModelProto trainingInfo */
    trainingInfo?: (onnx.ITrainingInfoProto[]|null);

    /** ModelProto functions */
    functions?: (onnx.IFunctionProto[]|null);
  }

  /** Represents a ModelProto. */
  class ModelProto implements IModelProto {
    /**
     * Constructs a new ModelProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IModelProto);

    /** ModelProto irVersion. */
    public irVersion: (number|Long);

    /** ModelProto opsetImport. */
    public opsetImport: onnx.IOperatorSetIdProto[];

    /** ModelProto producerName. */
    public producerName: string;

    /** ModelProto producerVersion. */
    public producerVersion: string;

    /** ModelProto domain. */
    public domain: string;

    /** ModelProto modelVersion. */
    public modelVersion: (number|Long);

    /** ModelProto docString. */
    public docString: string;

    /** ModelProto graph. */
    public graph?: (onnx.IGraphProto|null);

    /** ModelProto metadataProps. */
    public metadataProps: onnx.IStringStringEntryProto[];

    /** ModelProto trainingInfo. */
    public trainingInfo: onnx.ITrainingInfoProto[];

    /** ModelProto functions. */
    public functions: onnx.IFunctionProto[];

    /**
     * Creates a new ModelProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns ModelProto instance
     */
    public static create(properties?: onnx.IModelProto): onnx.ModelProto;

    /**
     * Encodes the specified ModelProto message. Does not implicitly {@link onnx.ModelProto.verify|verify} messages.
     * @param message ModelProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IModelProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified ModelProto message, length delimited. Does not implicitly {@link
     * onnx.ModelProto.verify|verify} messages.
     * @param message ModelProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IModelProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a ModelProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns ModelProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.ModelProto;

    /**
     * Decodes a ModelProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns ModelProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.ModelProto;

    /**
     * Verifies a ModelProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a ModelProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns ModelProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.ModelProto;

    /**
     * Creates a plain object from a ModelProto message. Also converts values to other types if specified.
     * @param message ModelProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.ModelProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this ModelProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for ModelProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a StringStringEntryProto. */
  interface IStringStringEntryProto {
    /** StringStringEntryProto key */
    key?: (string|null);

    /** StringStringEntryProto value */
    value?: (string|null);
  }

  /** Represents a StringStringEntryProto. */
  class StringStringEntryProto implements IStringStringEntryProto {
    /**
     * Constructs a new StringStringEntryProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IStringStringEntryProto);

    /** StringStringEntryProto key. */
    public key: string;

    /** StringStringEntryProto value. */
    public value: string;

    /**
     * Creates a new StringStringEntryProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns StringStringEntryProto instance
     */
    public static create(properties?: onnx.IStringStringEntryProto): onnx.StringStringEntryProto;

    /**
     * Encodes the specified StringStringEntryProto message. Does not implicitly {@link
     * onnx.StringStringEntryProto.verify|verify} messages.
     * @param message StringStringEntryProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IStringStringEntryProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified StringStringEntryProto message, length delimited. Does not implicitly {@link
     * onnx.StringStringEntryProto.verify|verify} messages.
     * @param message StringStringEntryProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IStringStringEntryProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a StringStringEntryProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns StringStringEntryProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.StringStringEntryProto;

    /**
     * Decodes a StringStringEntryProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns StringStringEntryProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.StringStringEntryProto;

    /**
     * Verifies a StringStringEntryProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a StringStringEntryProto message from a plain object. Also converts values to their respective internal
     * types.
     * @param object Plain object
     * @returns StringStringEntryProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.StringStringEntryProto;

    /**
     * Creates a plain object from a StringStringEntryProto message. Also converts values to other types if specified.
     * @param message StringStringEntryProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.StringStringEntryProto, options?: $protobuf.IConversionOptions):
        {[k: string]: any};

    /**
     * Converts this StringStringEntryProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for StringStringEntryProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a TensorAnnotation. */
  interface ITensorAnnotation {
    /** TensorAnnotation tensorName */
    tensorName?: (string|null);

    /** TensorAnnotation quantParameterTensorNames */
    quantParameterTensorNames?: (onnx.IStringStringEntryProto[]|null);
  }

  /** Represents a TensorAnnotation. */
  class TensorAnnotation implements ITensorAnnotation {
    /**
     * Constructs a new TensorAnnotation.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.ITensorAnnotation);

    /** TensorAnnotation tensorName. */
    public tensorName: string;

    /** TensorAnnotation quantParameterTensorNames. */
    public quantParameterTensorNames: onnx.IStringStringEntryProto[];

    /**
     * Creates a new TensorAnnotation instance using the specified properties.
     * @param [properties] Properties to set
     * @returns TensorAnnotation instance
     */
    public static create(properties?: onnx.ITensorAnnotation): onnx.TensorAnnotation;

    /**
     * Encodes the specified TensorAnnotation message. Does not implicitly {@link onnx.TensorAnnotation.verify|verify}
     * messages.
     * @param message TensorAnnotation message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.ITensorAnnotation, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified TensorAnnotation message, length delimited. Does not implicitly {@link
     * onnx.TensorAnnotation.verify|verify} messages.
     * @param message TensorAnnotation message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.ITensorAnnotation, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a TensorAnnotation message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns TensorAnnotation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TensorAnnotation;

    /**
     * Decodes a TensorAnnotation message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns TensorAnnotation
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TensorAnnotation;

    /**
     * Verifies a TensorAnnotation message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a TensorAnnotation message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns TensorAnnotation
     */
    public static fromObject(object: {[k: string]: any}): onnx.TensorAnnotation;

    /**
     * Creates a plain object from a TensorAnnotation message. Also converts values to other types if specified.
     * @param message TensorAnnotation
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.TensorAnnotation, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this TensorAnnotation to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for TensorAnnotation
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a GraphProto. */
  interface IGraphProto {
    /** GraphProto node */
    node?: (onnx.INodeProto[]|null);

    /** GraphProto name */
    name?: (string|null);

    /** GraphProto initializer */
    initializer?: (onnx.ITensorProto[]|null);

    /** GraphProto sparseInitializer */
    sparseInitializer?: (onnx.ISparseTensorProto[]|null);

    /** GraphProto docString */
    docString?: (string|null);

    /** GraphProto input */
    input?: (onnx.IValueInfoProto[]|null);

    /** GraphProto output */
    output?: (onnx.IValueInfoProto[]|null);

    /** GraphProto valueInfo */
    valueInfo?: (onnx.IValueInfoProto[]|null);

    /** GraphProto quantizationAnnotation */
    quantizationAnnotation?: (onnx.ITensorAnnotation[]|null);
  }

  /** Represents a GraphProto. */
  class GraphProto implements IGraphProto {
    /**
     * Constructs a new GraphProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IGraphProto);

    /** GraphProto node. */
    public node: onnx.INodeProto[];

    /** GraphProto name. */
    public name: string;

    /** GraphProto initializer. */
    public initializer: onnx.ITensorProto[];

    /** GraphProto sparseInitializer. */
    public sparseInitializer: onnx.ISparseTensorProto[];

    /** GraphProto docString. */
    public docString: string;

    /** GraphProto input. */
    public input: onnx.IValueInfoProto[];

    /** GraphProto output. */
    public output: onnx.IValueInfoProto[];

    /** GraphProto valueInfo. */
    public valueInfo: onnx.IValueInfoProto[];

    /** GraphProto quantizationAnnotation. */
    public quantizationAnnotation: onnx.ITensorAnnotation[];

    /**
     * Creates a new GraphProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns GraphProto instance
     */
    public static create(properties?: onnx.IGraphProto): onnx.GraphProto;

    /**
     * Encodes the specified GraphProto message. Does not implicitly {@link onnx.GraphProto.verify|verify} messages.
     * @param message GraphProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IGraphProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified GraphProto message, length delimited. Does not implicitly {@link
     * onnx.GraphProto.verify|verify} messages.
     * @param message GraphProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IGraphProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a GraphProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns GraphProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.GraphProto;

    /**
     * Decodes a GraphProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns GraphProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.GraphProto;

    /**
     * Verifies a GraphProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a GraphProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns GraphProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.GraphProto;

    /**
     * Creates a plain object from a GraphProto message. Also converts values to other types if specified.
     * @param message GraphProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.GraphProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this GraphProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for GraphProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a TensorProto. */
  interface ITensorProto {
    /** TensorProto dims */
    dims?: ((number | Long)[]|null);

    /** TensorProto dataType */
    dataType?: (number|null);

    /** TensorProto segment */
    segment?: (onnx.TensorProto.ISegment|null);

    /** TensorProto floatData */
    floatData?: (number[]|null);

    /** TensorProto int32Data */
    int32Data?: (number[]|null);

    /** TensorProto stringData */
    stringData?: (Uint8Array[]|null);

    /** TensorProto int64Data */
    int64Data?: ((number | Long)[]|null);

    /** TensorProto name */
    name?: (string|null);

    /** TensorProto docString */
    docString?: (string|null);

    /** TensorProto rawData */
    rawData?: (Uint8Array|null);

    /** TensorProto externalData */
    externalData?: (onnx.IStringStringEntryProto[]|null);

    /** TensorProto dataLocation */
    dataLocation?: (onnx.TensorProto.DataLocation|null);

    /** TensorProto doubleData */
    doubleData?: (number[]|null);

    /** TensorProto uint64Data */
    uint64Data?: ((number | Long)[]|null);
  }

  /** Represents a TensorProto. */
  class TensorProto implements ITensorProto {
    /**
     * Constructs a new TensorProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.ITensorProto);

    /** TensorProto dims. */
    public dims: (number|Long)[];

    /** TensorProto dataType. */
    public dataType: number;

    /** TensorProto segment. */
    public segment?: (onnx.TensorProto.ISegment|null);

    /** TensorProto floatData. */
    public floatData: number[];

    /** TensorProto int32Data. */
    public int32Data: number[];

    /** TensorProto stringData. */
    public stringData: Uint8Array[];

    /** TensorProto int64Data. */
    public int64Data: (number|Long)[];

    /** TensorProto name. */
    public name: string;

    /** TensorProto docString. */
    public docString: string;

    /** TensorProto rawData. */
    public rawData: Uint8Array;

    /** TensorProto externalData. */
    public externalData: onnx.IStringStringEntryProto[];

    /** TensorProto dataLocation. */
    public dataLocation: onnx.TensorProto.DataLocation;

    /** TensorProto doubleData. */
    public doubleData: number[];

    /** TensorProto uint64Data. */
    public uint64Data: (number|Long)[];

    /**
     * Creates a new TensorProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns TensorProto instance
     */
    public static create(properties?: onnx.ITensorProto): onnx.TensorProto;

    /**
     * Encodes the specified TensorProto message. Does not implicitly {@link onnx.TensorProto.verify|verify} messages.
     * @param message TensorProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.ITensorProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified TensorProto message, length delimited. Does not implicitly {@link
     * onnx.TensorProto.verify|verify} messages.
     * @param message TensorProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.ITensorProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a TensorProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns TensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TensorProto;

    /**
     * Decodes a TensorProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns TensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TensorProto;

    /**
     * Verifies a TensorProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a TensorProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns TensorProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.TensorProto;

    /**
     * Creates a plain object from a TensorProto message. Also converts values to other types if specified.
     * @param message TensorProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.TensorProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this TensorProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for TensorProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  namespace TensorProto {

    /** DataType enum. */
    enum DataType {
      UNDEFINED = 0,
      FLOAT = 1,
      UINT8 = 2,
      INT8 = 3,
      UINT16 = 4,
      INT16 = 5,
      INT32 = 6,
      INT64 = 7,
      STRING = 8,
      BOOL = 9,
      FLOAT16 = 10,
      DOUBLE = 11,
      UINT32 = 12,
      UINT64 = 13,
      COMPLEX64 = 14,
      COMPLEX128 = 15,
      BFLOAT16 = 16,
      FLOAT8E4M3FN = 17,
      FLOAT8E4M3FNUZ = 18,
      FLOAT8E5M2 = 19,
      FLOAT8E5M2FNUZ = 20
    }

    /** Properties of a Segment. */
    interface ISegment {
      /** Segment begin */
      begin?: (number|Long|null);

      /** Segment end */
      end?: (number|Long|null);
    }

    /** Represents a Segment. */
    class Segment implements ISegment {
      /**
       * Constructs a new Segment.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TensorProto.ISegment);

      /** Segment begin. */
      public begin: (number|Long);

      /** Segment end. */
      public end: (number|Long);

      /**
       * Creates a new Segment instance using the specified properties.
       * @param [properties] Properties to set
       * @returns Segment instance
       */
      public static create(properties?: onnx.TensorProto.ISegment): onnx.TensorProto.Segment;

      /**
       * Encodes the specified Segment message. Does not implicitly {@link onnx.TensorProto.Segment.verify|verify}
       * messages.
       * @param message Segment message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TensorProto.ISegment, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified Segment message, length delimited. Does not implicitly {@link
       * onnx.TensorProto.Segment.verify|verify} messages.
       * @param message Segment message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TensorProto.ISegment, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Decodes a Segment message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns Segment
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TensorProto.Segment;

      /**
       * Decodes a Segment message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns Segment
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TensorProto.Segment;

      /**
       * Verifies a Segment message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates a Segment message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns Segment
       */
      public static fromObject(object: {[k: string]: any}): onnx.TensorProto.Segment;

      /**
       * Creates a plain object from a Segment message. Also converts values to other types if specified.
       * @param message Segment
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TensorProto.Segment, options?: $protobuf.IConversionOptions):
          {[k: string]: any};

      /**
       * Converts this Segment to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for Segment
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }

    /** DataLocation enum. */
    enum DataLocation { DEFAULT = 0, EXTERNAL = 1 }
  }

  /** Properties of a SparseTensorProto. */
  interface ISparseTensorProto {
    /** SparseTensorProto values */
    values?: (onnx.ITensorProto|null);

    /** SparseTensorProto indices */
    indices?: (onnx.ITensorProto|null);

    /** SparseTensorProto dims */
    dims?: ((number | Long)[]|null);
  }

  /** Represents a SparseTensorProto. */
  class SparseTensorProto implements ISparseTensorProto {
    /**
     * Constructs a new SparseTensorProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.ISparseTensorProto);

    /** SparseTensorProto values. */
    public values?: (onnx.ITensorProto|null);

    /** SparseTensorProto indices. */
    public indices?: (onnx.ITensorProto|null);

    /** SparseTensorProto dims. */
    public dims: (number|Long)[];

    /**
     * Creates a new SparseTensorProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns SparseTensorProto instance
     */
    public static create(properties?: onnx.ISparseTensorProto): onnx.SparseTensorProto;

    /**
     * Encodes the specified SparseTensorProto message. Does not implicitly {@link onnx.SparseTensorProto.verify|verify}
     * messages.
     * @param message SparseTensorProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.ISparseTensorProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified SparseTensorProto message, length delimited. Does not implicitly {@link
     * onnx.SparseTensorProto.verify|verify} messages.
     * @param message SparseTensorProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.ISparseTensorProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a SparseTensorProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns SparseTensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.SparseTensorProto;

    /**
     * Decodes a SparseTensorProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns SparseTensorProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.SparseTensorProto;

    /**
     * Verifies a SparseTensorProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a SparseTensorProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns SparseTensorProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.SparseTensorProto;

    /**
     * Creates a plain object from a SparseTensorProto message. Also converts values to other types if specified.
     * @param message SparseTensorProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.SparseTensorProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this SparseTensorProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for SparseTensorProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** Properties of a TensorShapeProto. */
  interface ITensorShapeProto {
    /** TensorShapeProto dim */
    dim?: (onnx.TensorShapeProto.IDimension[]|null);
  }

  /** Represents a TensorShapeProto. */
  class TensorShapeProto implements ITensorShapeProto {
    /**
     * Constructs a new TensorShapeProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.ITensorShapeProto);

    /** TensorShapeProto dim. */
    public dim: onnx.TensorShapeProto.IDimension[];

    /**
     * Creates a new TensorShapeProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns TensorShapeProto instance
     */
    public static create(properties?: onnx.ITensorShapeProto): onnx.TensorShapeProto;

    /**
     * Encodes the specified TensorShapeProto message. Does not implicitly {@link onnx.TensorShapeProto.verify|verify}
     * messages.
     * @param message TensorShapeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.ITensorShapeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified TensorShapeProto message, length delimited. Does not implicitly {@link
     * onnx.TensorShapeProto.verify|verify} messages.
     * @param message TensorShapeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.ITensorShapeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a TensorShapeProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns TensorShapeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TensorShapeProto;

    /**
     * Decodes a TensorShapeProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns TensorShapeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TensorShapeProto;

    /**
     * Verifies a TensorShapeProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a TensorShapeProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns TensorShapeProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.TensorShapeProto;

    /**
     * Creates a plain object from a TensorShapeProto message. Also converts values to other types if specified.
     * @param message TensorShapeProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.TensorShapeProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this TensorShapeProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for TensorShapeProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  namespace TensorShapeProto {

    /** Properties of a Dimension. */
    interface IDimension {
      /** Dimension dimValue */
      dimValue?: (number|Long|null);

      /** Dimension dimParam */
      dimParam?: (string|null);

      /** Dimension denotation */
      denotation?: (string|null);
    }

    /** Represents a Dimension. */
    class Dimension implements IDimension {
      /**
       * Constructs a new Dimension.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TensorShapeProto.IDimension);

      /** Dimension dimValue. */
      public dimValue?: (number|Long|null);

      /** Dimension dimParam. */
      public dimParam?: (string|null);

      /** Dimension denotation. */
      public denotation: string;

      /** Dimension value. */
      public value?: ('dimValue'|'dimParam');

      /**
       * Creates a new Dimension instance using the specified properties.
       * @param [properties] Properties to set
       * @returns Dimension instance
       */
      public static create(properties?: onnx.TensorShapeProto.IDimension): onnx.TensorShapeProto.Dimension;

      /**
       * Encodes the specified Dimension message. Does not implicitly {@link
       * onnx.TensorShapeProto.Dimension.verify|verify} messages.
       * @param message Dimension message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TensorShapeProto.IDimension, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified Dimension message, length delimited. Does not implicitly {@link
       * onnx.TensorShapeProto.Dimension.verify|verify} messages.
       * @param message Dimension message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TensorShapeProto.IDimension, writer?: $protobuf.Writer):
          $protobuf.Writer;

      /**
       * Decodes a Dimension message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns Dimension
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TensorShapeProto.Dimension;

      /**
       * Decodes a Dimension message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns Dimension
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TensorShapeProto.Dimension;

      /**
       * Verifies a Dimension message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates a Dimension message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns Dimension
       */
      public static fromObject(object: {[k: string]: any}): onnx.TensorShapeProto.Dimension;

      /**
       * Creates a plain object from a Dimension message. Also converts values to other types if specified.
       * @param message Dimension
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TensorShapeProto.Dimension, options?: $protobuf.IConversionOptions):
          {[k: string]: any};

      /**
       * Converts this Dimension to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for Dimension
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }
  }

  /** Properties of a TypeProto. */
  interface ITypeProto {
    /** TypeProto tensorType */
    tensorType?: (onnx.TypeProto.ITensor|null);

    /** TypeProto sequenceType */
    sequenceType?: (onnx.TypeProto.ISequence|null);

    /** TypeProto mapType */
    mapType?: (onnx.TypeProto.IMap|null);

    /** TypeProto optionalType */
    optionalType?: (onnx.TypeProto.IOptional|null);

    /** TypeProto sparseTensorType */
    sparseTensorType?: (onnx.TypeProto.ISparseTensor|null);

    /** TypeProto denotation */
    denotation?: (string|null);
  }

  /** Represents a TypeProto. */
  class TypeProto implements ITypeProto {
    /**
     * Constructs a new TypeProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.ITypeProto);

    /** TypeProto tensorType. */
    public tensorType?: (onnx.TypeProto.ITensor|null);

    /** TypeProto sequenceType. */
    public sequenceType?: (onnx.TypeProto.ISequence|null);

    /** TypeProto mapType. */
    public mapType?: (onnx.TypeProto.IMap|null);

    /** TypeProto optionalType. */
    public optionalType?: (onnx.TypeProto.IOptional|null);

    /** TypeProto sparseTensorType. */
    public sparseTensorType?: (onnx.TypeProto.ISparseTensor|null);

    /** TypeProto denotation. */
    public denotation: string;

    /** TypeProto value. */
    public value?: ('tensorType'|'sequenceType'|'mapType'|'optionalType'|'sparseTensorType');

    /**
     * Creates a new TypeProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns TypeProto instance
     */
    public static create(properties?: onnx.ITypeProto): onnx.TypeProto;

    /**
     * Encodes the specified TypeProto message. Does not implicitly {@link onnx.TypeProto.verify|verify} messages.
     * @param message TypeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.ITypeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified TypeProto message, length delimited. Does not implicitly {@link
     * onnx.TypeProto.verify|verify} messages.
     * @param message TypeProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.ITypeProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a TypeProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns TypeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TypeProto;

    /**
     * Decodes a TypeProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns TypeProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TypeProto;

    /**
     * Verifies a TypeProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a TypeProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns TypeProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.TypeProto;

    /**
     * Creates a plain object from a TypeProto message. Also converts values to other types if specified.
     * @param message TypeProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.TypeProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this TypeProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for TypeProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  namespace TypeProto {

    /** Properties of a Tensor. */
    interface ITensor {
      /** Tensor elemType */
      elemType?: (number|null);

      /** Tensor shape */
      shape?: (onnx.ITensorShapeProto|null);
    }

    /** Represents a Tensor. */
    class Tensor implements ITensor {
      /**
       * Constructs a new Tensor.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TypeProto.ITensor);

      /** Tensor elemType. */
      public elemType: number;

      /** Tensor shape. */
      public shape?: (onnx.ITensorShapeProto|null);

      /**
       * Creates a new Tensor instance using the specified properties.
       * @param [properties] Properties to set
       * @returns Tensor instance
       */
      public static create(properties?: onnx.TypeProto.ITensor): onnx.TypeProto.Tensor;

      /**
       * Encodes the specified Tensor message. Does not implicitly {@link onnx.TypeProto.Tensor.verify|verify} messages.
       * @param message Tensor message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TypeProto.ITensor, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified Tensor message, length delimited. Does not implicitly {@link
       * onnx.TypeProto.Tensor.verify|verify} messages.
       * @param message Tensor message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TypeProto.ITensor, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Decodes a Tensor message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns Tensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TypeProto.Tensor;

      /**
       * Decodes a Tensor message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns Tensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TypeProto.Tensor;

      /**
       * Verifies a Tensor message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates a Tensor message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns Tensor
       */
      public static fromObject(object: {[k: string]: any}): onnx.TypeProto.Tensor;

      /**
       * Creates a plain object from a Tensor message. Also converts values to other types if specified.
       * @param message Tensor
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TypeProto.Tensor, options?: $protobuf.IConversionOptions):
          {[k: string]: any};

      /**
       * Converts this Tensor to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for Tensor
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }

    /** Properties of a Sequence. */
    interface ISequence {
      /** Sequence elemType */
      elemType?: (onnx.ITypeProto|null);
    }

    /** Represents a Sequence. */
    class Sequence implements ISequence {
      /**
       * Constructs a new Sequence.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TypeProto.ISequence);

      /** Sequence elemType. */
      public elemType?: (onnx.ITypeProto|null);

      /**
       * Creates a new Sequence instance using the specified properties.
       * @param [properties] Properties to set
       * @returns Sequence instance
       */
      public static create(properties?: onnx.TypeProto.ISequence): onnx.TypeProto.Sequence;

      /**
       * Encodes the specified Sequence message. Does not implicitly {@link onnx.TypeProto.Sequence.verify|verify}
       * messages.
       * @param message Sequence message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TypeProto.ISequence, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified Sequence message, length delimited. Does not implicitly {@link
       * onnx.TypeProto.Sequence.verify|verify} messages.
       * @param message Sequence message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TypeProto.ISequence, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Decodes a Sequence message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns Sequence
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TypeProto.Sequence;

      /**
       * Decodes a Sequence message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns Sequence
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TypeProto.Sequence;

      /**
       * Verifies a Sequence message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates a Sequence message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns Sequence
       */
      public static fromObject(object: {[k: string]: any}): onnx.TypeProto.Sequence;

      /**
       * Creates a plain object from a Sequence message. Also converts values to other types if specified.
       * @param message Sequence
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TypeProto.Sequence, options?: $protobuf.IConversionOptions):
          {[k: string]: any};

      /**
       * Converts this Sequence to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for Sequence
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }

    /** Properties of a Map. */
    interface IMap {
      /** Map keyType */
      keyType?: (number|null);

      /** Map valueType */
      valueType?: (onnx.ITypeProto|null);
    }

    /** Represents a Map. */
    class Map implements IMap {
      /**
       * Constructs a new Map.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TypeProto.IMap);

      /** Map keyType. */
      public keyType: number;

      /** Map valueType. */
      public valueType?: (onnx.ITypeProto|null);

      /**
       * Creates a new Map instance using the specified properties.
       * @param [properties] Properties to set
       * @returns Map instance
       */
      public static create(properties?: onnx.TypeProto.IMap): onnx.TypeProto.Map;

      /**
       * Encodes the specified Map message. Does not implicitly {@link onnx.TypeProto.Map.verify|verify} messages.
       * @param message Map message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TypeProto.IMap, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified Map message, length delimited. Does not implicitly {@link
       * onnx.TypeProto.Map.verify|verify} messages.
       * @param message Map message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TypeProto.IMap, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Decodes a Map message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns Map
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TypeProto.Map;

      /**
       * Decodes a Map message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns Map
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TypeProto.Map;

      /**
       * Verifies a Map message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates a Map message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns Map
       */
      public static fromObject(object: {[k: string]: any}): onnx.TypeProto.Map;

      /**
       * Creates a plain object from a Map message. Also converts values to other types if specified.
       * @param message Map
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TypeProto.Map, options?: $protobuf.IConversionOptions): {[k: string]: any};

      /**
       * Converts this Map to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for Map
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }

    /** Properties of an Optional. */
    interface IOptional {
      /** Optional elemType */
      elemType?: (onnx.ITypeProto|null);
    }

    /** Represents an Optional. */
    class Optional implements IOptional {
      /**
       * Constructs a new Optional.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TypeProto.IOptional);

      /** Optional elemType. */
      public elemType?: (onnx.ITypeProto|null);

      /**
       * Creates a new Optional instance using the specified properties.
       * @param [properties] Properties to set
       * @returns Optional instance
       */
      public static create(properties?: onnx.TypeProto.IOptional): onnx.TypeProto.Optional;

      /**
       * Encodes the specified Optional message. Does not implicitly {@link onnx.TypeProto.Optional.verify|verify}
       * messages.
       * @param message Optional message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TypeProto.IOptional, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified Optional message, length delimited. Does not implicitly {@link
       * onnx.TypeProto.Optional.verify|verify} messages.
       * @param message Optional message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TypeProto.IOptional, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Decodes an Optional message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns Optional
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TypeProto.Optional;

      /**
       * Decodes an Optional message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns Optional
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TypeProto.Optional;

      /**
       * Verifies an Optional message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates an Optional message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns Optional
       */
      public static fromObject(object: {[k: string]: any}): onnx.TypeProto.Optional;

      /**
       * Creates a plain object from an Optional message. Also converts values to other types if specified.
       * @param message Optional
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TypeProto.Optional, options?: $protobuf.IConversionOptions):
          {[k: string]: any};

      /**
       * Converts this Optional to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for Optional
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }

    /** Properties of a SparseTensor. */
    interface ISparseTensor {
      /** SparseTensor elemType */
      elemType?: (number|null);

      /** SparseTensor shape */
      shape?: (onnx.ITensorShapeProto|null);
    }

    /** Represents a SparseTensor. */
    class SparseTensor implements ISparseTensor {
      /**
       * Constructs a new SparseTensor.
       * @param [properties] Properties to set
       */
      constructor(properties?: onnx.TypeProto.ISparseTensor);

      /** SparseTensor elemType. */
      public elemType: number;

      /** SparseTensor shape. */
      public shape?: (onnx.ITensorShapeProto|null);

      /**
       * Creates a new SparseTensor instance using the specified properties.
       * @param [properties] Properties to set
       * @returns SparseTensor instance
       */
      public static create(properties?: onnx.TypeProto.ISparseTensor): onnx.TypeProto.SparseTensor;

      /**
       * Encodes the specified SparseTensor message. Does not implicitly {@link
       * onnx.TypeProto.SparseTensor.verify|verify} messages.
       * @param message SparseTensor message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encode(message: onnx.TypeProto.ISparseTensor, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Encodes the specified SparseTensor message, length delimited. Does not implicitly {@link
       * onnx.TypeProto.SparseTensor.verify|verify} messages.
       * @param message SparseTensor message or plain object to encode
       * @param [writer] Writer to encode to
       * @returns Writer
       */
      public static encodeDelimited(message: onnx.TypeProto.ISparseTensor, writer?: $protobuf.Writer): $protobuf.Writer;

      /**
       * Decodes a SparseTensor message from the specified reader or buffer.
       * @param reader Reader or buffer to decode from
       * @param [length] Message length if known beforehand
       * @returns SparseTensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.TypeProto.SparseTensor;

      /**
       * Decodes a SparseTensor message from the specified reader or buffer, length delimited.
       * @param reader Reader or buffer to decode from
       * @returns SparseTensor
       * @throws {Error} If the payload is not a reader or valid buffer
       * @throws {$protobuf.util.ProtocolError} If required fields are missing
       */
      public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.TypeProto.SparseTensor;

      /**
       * Verifies a SparseTensor message.
       * @param message Plain object to verify
       * @returns `null` if valid, otherwise the reason why it is not
       */
      public static verify(message: {[k: string]: any}): (string|null);

      /**
       * Creates a SparseTensor message from a plain object. Also converts values to their respective internal types.
       * @param object Plain object
       * @returns SparseTensor
       */
      public static fromObject(object: {[k: string]: any}): onnx.TypeProto.SparseTensor;

      /**
       * Creates a plain object from a SparseTensor message. Also converts values to other types if specified.
       * @param message SparseTensor
       * @param [options] Conversion options
       * @returns Plain object
       */
      public static toObject(message: onnx.TypeProto.SparseTensor, options?: $protobuf.IConversionOptions):
          {[k: string]: any};

      /**
       * Converts this SparseTensor to JSON.
       * @returns JSON object
       */
      public toJSON(): {[k: string]: any};

      /**
       * Gets the default type url for SparseTensor
       * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
       * @returns The default type url
       */
      public static getTypeUrl(typeUrlPrefix?: string): string;
    }
  }

  /** Properties of an OperatorSetIdProto. */
  interface IOperatorSetIdProto {
    /** OperatorSetIdProto domain */
    domain?: (string|null);

    /** OperatorSetIdProto version */
    version?: (number|Long|null);
  }

  /** Represents an OperatorSetIdProto. */
  class OperatorSetIdProto implements IOperatorSetIdProto {
    /**
     * Constructs a new OperatorSetIdProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IOperatorSetIdProto);

    /** OperatorSetIdProto domain. */
    public domain: string;

    /** OperatorSetIdProto version. */
    public version: (number|Long);

    /**
     * Creates a new OperatorSetIdProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns OperatorSetIdProto instance
     */
    public static create(properties?: onnx.IOperatorSetIdProto): onnx.OperatorSetIdProto;

    /**
     * Encodes the specified OperatorSetIdProto message. Does not implicitly {@link
     * onnx.OperatorSetIdProto.verify|verify} messages.
     * @param message OperatorSetIdProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IOperatorSetIdProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified OperatorSetIdProto message, length delimited. Does not implicitly {@link
     * onnx.OperatorSetIdProto.verify|verify} messages.
     * @param message OperatorSetIdProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IOperatorSetIdProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes an OperatorSetIdProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns OperatorSetIdProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.OperatorSetIdProto;

    /**
     * Decodes an OperatorSetIdProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns OperatorSetIdProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.OperatorSetIdProto;

    /**
     * Verifies an OperatorSetIdProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates an OperatorSetIdProto message from a plain object. Also converts values to their respective internal
     * types.
     * @param object Plain object
     * @returns OperatorSetIdProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.OperatorSetIdProto;

    /**
     * Creates a plain object from an OperatorSetIdProto message. Also converts values to other types if specified.
     * @param message OperatorSetIdProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.OperatorSetIdProto, options?: $protobuf.IConversionOptions):
        {[k: string]: any};

    /**
     * Converts this OperatorSetIdProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for OperatorSetIdProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }

  /** OperatorStatus enum. */
  enum OperatorStatus { EXPERIMENTAL = 0, STABLE = 1 }

  /** Properties of a FunctionProto. */
  interface IFunctionProto {
    /** FunctionProto name */
    name?: (string|null);

    /** FunctionProto input */
    input?: (string[]|null);

    /** FunctionProto output */
    output?: (string[]|null);

    /** FunctionProto attribute */
    attribute?: (string[]|null);

    /** FunctionProto attributeProto */
    attributeProto?: (onnx.IAttributeProto[]|null);

    /** FunctionProto node */
    node?: (onnx.INodeProto[]|null);

    /** FunctionProto docString */
    docString?: (string|null);

    /** FunctionProto opsetImport */
    opsetImport?: (onnx.IOperatorSetIdProto[]|null);

    /** FunctionProto domain */
    domain?: (string|null);
  }

  /** Represents a FunctionProto. */
  class FunctionProto implements IFunctionProto {
    /**
     * Constructs a new FunctionProto.
     * @param [properties] Properties to set
     */
    constructor(properties?: onnx.IFunctionProto);

    /** FunctionProto name. */
    public name: string;

    /** FunctionProto input. */
    public input: string[];

    /** FunctionProto output. */
    public output: string[];

    /** FunctionProto attribute. */
    public attribute: string[];

    /** FunctionProto attributeProto. */
    public attributeProto: onnx.IAttributeProto[];

    /** FunctionProto node. */
    public node: onnx.INodeProto[];

    /** FunctionProto docString. */
    public docString: string;

    /** FunctionProto opsetImport. */
    public opsetImport: onnx.IOperatorSetIdProto[];

    /** FunctionProto domain. */
    public domain: string;

    /**
     * Creates a new FunctionProto instance using the specified properties.
     * @param [properties] Properties to set
     * @returns FunctionProto instance
     */
    public static create(properties?: onnx.IFunctionProto): onnx.FunctionProto;

    /**
     * Encodes the specified FunctionProto message. Does not implicitly {@link onnx.FunctionProto.verify|verify}
     * messages.
     * @param message FunctionProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encode(message: onnx.IFunctionProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Encodes the specified FunctionProto message, length delimited. Does not implicitly {@link
     * onnx.FunctionProto.verify|verify} messages.
     * @param message FunctionProto message or plain object to encode
     * @param [writer] Writer to encode to
     * @returns Writer
     */
    public static encodeDelimited(message: onnx.IFunctionProto, writer?: $protobuf.Writer): $protobuf.Writer;

    /**
     * Decodes a FunctionProto message from the specified reader or buffer.
     * @param reader Reader or buffer to decode from
     * @param [length] Message length if known beforehand
     * @returns FunctionProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): onnx.FunctionProto;

    /**
     * Decodes a FunctionProto message from the specified reader or buffer, length delimited.
     * @param reader Reader or buffer to decode from
     * @returns FunctionProto
     * @throws {Error} If the payload is not a reader or valid buffer
     * @throws {$protobuf.util.ProtocolError} If required fields are missing
     */
    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): onnx.FunctionProto;

    /**
     * Verifies a FunctionProto message.
     * @param message Plain object to verify
     * @returns `null` if valid, otherwise the reason why it is not
     */
    public static verify(message: {[k: string]: any}): (string|null);

    /**
     * Creates a FunctionProto message from a plain object. Also converts values to their respective internal types.
     * @param object Plain object
     * @returns FunctionProto
     */
    public static fromObject(object: {[k: string]: any}): onnx.FunctionProto;

    /**
     * Creates a plain object from a FunctionProto message. Also converts values to other types if specified.
     * @param message FunctionProto
     * @param [options] Conversion options
     * @returns Plain object
     */
    public static toObject(message: onnx.FunctionProto, options?: $protobuf.IConversionOptions): {[k: string]: any};

    /**
     * Converts this FunctionProto to JSON.
     * @returns JSON object
     */
    public toJSON(): {[k: string]: any};

    /**
     * Gets the default type url for FunctionProto
     * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
     * @returns The default type url
     */
    public static getTypeUrl(typeUrlPrefix?: string): string;
  }
}
