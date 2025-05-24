export function fromUTF8Array(data) {
    const decoder = new TextDecoder();
    return decoder.decode(data);
}
export function toUTF8Array(str) {
    const encoder = new TextEncoder();
    return encoder.encode(str);
}
