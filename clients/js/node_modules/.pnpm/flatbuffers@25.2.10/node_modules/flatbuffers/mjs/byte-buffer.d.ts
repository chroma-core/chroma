import { Offset, Table, IGeneratedObject, IUnpackableObject } from "./types.js";
import { Encoding } from "./encoding.js";
export declare class ByteBuffer {
    private bytes_;
    private position_;
    private text_decoder_;
    /**
     * Create a new ByteBuffer with a given array of bytes (`Uint8Array`)
     */
    constructor(bytes_: Uint8Array);
    /**
     * Create and allocate a new ByteBuffer with a given size.
     */
    static allocate(byte_size: number): ByteBuffer;
    clear(): void;
    /**
     * Get the underlying `Uint8Array`.
     */
    bytes(): Uint8Array;
    /**
     * Get the buffer's position.
     */
    position(): number;
    /**
     * Set the buffer's position.
     */
    setPosition(position: number): void;
    /**
     * Get the buffer's capacity.
     */
    capacity(): number;
    readInt8(offset: number): number;
    readUint8(offset: number): number;
    readInt16(offset: number): number;
    readUint16(offset: number): number;
    readInt32(offset: number): number;
    readUint32(offset: number): number;
    readInt64(offset: number): bigint;
    readUint64(offset: number): bigint;
    readFloat32(offset: number): number;
    readFloat64(offset: number): number;
    writeInt8(offset: number, value: number): void;
    writeUint8(offset: number, value: number): void;
    writeInt16(offset: number, value: number): void;
    writeUint16(offset: number, value: number): void;
    writeInt32(offset: number, value: number): void;
    writeUint32(offset: number, value: number): void;
    writeInt64(offset: number, value: bigint): void;
    writeUint64(offset: number, value: bigint): void;
    writeFloat32(offset: number, value: number): void;
    writeFloat64(offset: number, value: number): void;
    /**
     * Return the file identifier.   Behavior is undefined for FlatBuffers whose
     * schema does not include a file_identifier (likely points at padding or the
     * start of a the root vtable).
     */
    getBufferIdentifier(): string;
    /**
     * Look up a field in the vtable, return an offset into the object, or 0 if the
     * field is not present.
     */
    __offset(bb_pos: number, vtable_offset: number): Offset;
    /**
     * Initialize any Table-derived type to point to the union at the given offset.
     */
    __union(t: Table, offset: number): Table;
    /**
     * Create a JavaScript string from UTF-8 data stored inside the FlatBuffer.
     * This allocates a new string and converts to wide chars upon each access.
     *
     * To avoid the conversion to string, pass Encoding.UTF8_BYTES as the
     * "optionalEncoding" argument. This is useful for avoiding conversion when
     * the data will just be packaged back up in another FlatBuffer later on.
     *
     * @param offset
     * @param opt_encoding Defaults to UTF16_STRING
     */
    __string(offset: number, opt_encoding?: Encoding): string | Uint8Array;
    /**
     * Handle unions that can contain string as its member, if a Table-derived type then initialize it,
     * if a string then return a new one
     *
     * WARNING: strings are immutable in JS so we can't change the string that the user gave us, this
     * makes the behaviour of __union_with_string different compared to __union
     */
    __union_with_string(o: Table | string, offset: number): Table | string;
    /**
     * Retrieve the relative offset stored at "offset"
     */
    __indirect(offset: Offset): Offset;
    /**
     * Get the start of data of a vector whose offset is stored at "offset" in this object.
     */
    __vector(offset: Offset): Offset;
    /**
     * Get the length of a vector whose offset is stored at "offset" in this object.
     */
    __vector_len(offset: Offset): Offset;
    __has_identifier(ident: string): boolean;
    /**
     * A helper function for generating list for obj api
     */
    createScalarList<T>(listAccessor: (i: number) => T | null, listLength: number): T[];
    /**
     * A helper function for generating list for obj api
     * @param listAccessor function that accepts an index and return data at that index
     * @param listLength listLength
     * @param res result list
     */
    createObjList<T1 extends IUnpackableObject<T2>, T2 extends IGeneratedObject>(listAccessor: (i: number) => T1 | null, listLength: number): T2[];
}
