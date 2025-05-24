import { ByteBuffer } from "./byte-buffer.js";
import { Offset, IGeneratedObject } from "./types.js";
export declare class Builder {
    private bb;
    /** Remaining space in the ByteBuffer. */
    private space;
    /** Minimum alignment encountered so far. */
    private minalign;
    /** The vtable for the current table. */
    private vtable;
    /** The amount of fields we're actually using. */
    private vtable_in_use;
    /** Whether we are currently serializing a table. */
    private isNested;
    /** Starting offset of the current struct/table. */
    private object_start;
    /** List of offsets of all vtables. */
    private vtables;
    /** For the current vector being built. */
    private vector_num_elems;
    /** False omits default values from the serialized data */
    private force_defaults;
    private string_maps;
    private text_encoder;
    /**
     * Create a FlatBufferBuilder.
     */
    constructor(opt_initial_size?: number);
    clear(): void;
    /**
     * In order to save space, fields that are set to their default value
     * don't get serialized into the buffer. Forcing defaults provides a
     * way to manually disable this optimization.
     *
     * @param forceDefaults true always serializes default values
     */
    forceDefaults(forceDefaults: boolean): void;
    /**
     * Get the ByteBuffer representing the FlatBuffer. Only call this after you've
     * called finish(). The actual data starts at the ByteBuffer's current position,
     * not necessarily at 0.
     */
    dataBuffer(): ByteBuffer;
    /**
     * Get the bytes representing the FlatBuffer. Only call this after you've
     * called finish().
     */
    asUint8Array(): Uint8Array;
    /**
     * Prepare to write an element of `size` after `additional_bytes` have been
     * written, e.g. if you write a string, you need to align such the int length
     * field is aligned to 4 bytes, and the string data follows it directly. If all
     * you need to do is alignment, `additional_bytes` will be 0.
     *
     * @param size This is the of the new element to write
     * @param additional_bytes The padding size
     */
    prep(size: number, additional_bytes: number): void;
    pad(byte_size: number): void;
    writeInt8(value: number): void;
    writeInt16(value: number): void;
    writeInt32(value: number): void;
    writeInt64(value: bigint): void;
    writeFloat32(value: number): void;
    writeFloat64(value: number): void;
    /**
     * Add an `int8` to the buffer, properly aligned, and grows the buffer (if necessary).
     * @param value The `int8` to add the buffer.
     */
    addInt8(value: number): void;
    /**
     * Add an `int16` to the buffer, properly aligned, and grows the buffer (if necessary).
     * @param value The `int16` to add the buffer.
     */
    addInt16(value: number): void;
    /**
     * Add an `int32` to the buffer, properly aligned, and grows the buffer (if necessary).
     * @param value The `int32` to add the buffer.
     */
    addInt32(value: number): void;
    /**
     * Add an `int64` to the buffer, properly aligned, and grows the buffer (if necessary).
     * @param value The `int64` to add the buffer.
     */
    addInt64(value: bigint): void;
    /**
     * Add a `float32` to the buffer, properly aligned, and grows the buffer (if necessary).
     * @param value The `float32` to add the buffer.
     */
    addFloat32(value: number): void;
    /**
     * Add a `float64` to the buffer, properly aligned, and grows the buffer (if necessary).
     * @param value The `float64` to add the buffer.
     */
    addFloat64(value: number): void;
    addFieldInt8(voffset: number, value: number, defaultValue: number | null): void;
    addFieldInt16(voffset: number, value: number, defaultValue: number | null): void;
    addFieldInt32(voffset: number, value: number, defaultValue: number | null): void;
    addFieldInt64(voffset: number, value: bigint, defaultValue: bigint | null): void;
    addFieldFloat32(voffset: number, value: number, defaultValue: number | null): void;
    addFieldFloat64(voffset: number, value: number, defaultValue: number | null): void;
    addFieldOffset(voffset: number, value: Offset, defaultValue: Offset): void;
    /**
     * Structs are stored inline, so nothing additional is being added. `d` is always 0.
     */
    addFieldStruct(voffset: number, value: Offset, defaultValue: Offset): void;
    /**
     * Structures are always stored inline, they need to be created right
     * where they're used.  You'll get this assertion failure if you
     * created it elsewhere.
     */
    nested(obj: Offset): void;
    /**
     * Should not be creating any other object, string or vector
     * while an object is being constructed
     */
    notNested(): void;
    /**
     * Set the current vtable at `voffset` to the current location in the buffer.
     */
    slot(voffset: number): void;
    /**
     * @returns Offset relative to the end of the buffer.
     */
    offset(): Offset;
    /**
     * Doubles the size of the backing ByteBuffer and copies the old data towards
     * the end of the new buffer (since we build the buffer backwards).
     *
     * @param bb The current buffer with the existing data
     * @returns A new byte buffer with the old data copied
     * to it. The data is located at the end of the buffer.
     *
     * uint8Array.set() formally takes {Array<number>|ArrayBufferView}, so to pass
     * it a uint8Array we need to suppress the type check:
     * @suppress {checkTypes}
     */
    static growByteBuffer(bb: ByteBuffer): ByteBuffer;
    /**
     * Adds on offset, relative to where it will be written.
     *
     * @param offset The offset to add.
     */
    addOffset(offset: Offset): void;
    /**
     * Start encoding a new object in the buffer.  Users will not usually need to
     * call this directly. The FlatBuffers compiler will generate helper methods
     * that call this method internally.
     */
    startObject(numfields: number): void;
    /**
     * Finish off writing the object that is under construction.
     *
     * @returns The offset to the object inside `dataBuffer`
     */
    endObject(): Offset;
    /**
     * Finalize a buffer, poiting to the given `root_table`.
     */
    finish(root_table: Offset, opt_file_identifier?: string, opt_size_prefix?: boolean): void;
    /**
     * Finalize a size prefixed buffer, pointing to the given `root_table`.
     */
    finishSizePrefixed(this: Builder, root_table: Offset, opt_file_identifier?: string): void;
    /**
     * This checks a required field has been set in a given table that has
     * just been constructed.
     */
    requiredField(table: Offset, field: number): void;
    /**
     * Start a new array/vector of objects.  Users usually will not call
     * this directly. The FlatBuffers compiler will create a start/end
     * method for vector types in generated code.
     *
     * @param elem_size The size of each element in the array
     * @param num_elems The number of elements in the array
     * @param alignment The alignment of the array
     */
    startVector(elem_size: number, num_elems: number, alignment: number): void;
    /**
     * Finish off the creation of an array and all its elements. The array must be
     * created with `startVector`.
     *
     * @returns The offset at which the newly created array
     * starts.
     */
    endVector(): Offset;
    /**
     * Encode the string `s` in the buffer using UTF-8. If the string passed has
     * already been seen, we return the offset of the already written string
     *
     * @param s The string to encode
     * @return The offset in the buffer where the encoded string starts
     */
    createSharedString(s: string | Uint8Array): Offset;
    /**
     * Encode the string `s` in the buffer using UTF-8. If a Uint8Array is passed
     * instead of a string, it is assumed to contain valid UTF-8 encoded data.
     *
     * @param s The string to encode
     * @return The offset in the buffer where the encoded string starts
     */
    createString(s: string | Uint8Array | null | undefined): Offset;
    /**
     * Create a byte vector.
     *
     * @param v The bytes to add
     * @returns The offset in the buffer where the byte vector starts
     */
    createByteVector(v: Uint8Array | null | undefined): Offset;
    /**
     * A helper function to pack an object
     *
     * @returns offset of obj
     */
    createObjectOffset(obj: string | IGeneratedObject | null): Offset;
    /**
     * A helper function to pack a list of object
     *
     * @returns list of offsets of each non null object
     */
    createObjectOffsetList(list: (string | IGeneratedObject)[]): Offset[];
    createStructOffsetList(list: (string | IGeneratedObject)[], startFunc: (builder: Builder, length: number) => void): Offset;
}
