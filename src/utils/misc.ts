import EventEmitter from "events";

export function portFromEndpoint(endpoint: string): number {
    return parseInt(endpoint.split(':')[2]);
}

/**
 * Simple object check.
 * https://stackoverflow.com/questions/27936772/how-to-deep-merge-instead-of-shallow-merge
 * @param item
 * @returns {boolean}
 */
function isObject(item) {
    return (item && typeof item === 'object' && !Array.isArray(item));
}

/**
 * Deep merge two objects.
 * @param target
 * @param ...sources
 */
export function mergeDeep(target, ...sources) {
    if (!sources.length) return target;
    const source = sources.shift();

    if (isObject(target) && isObject(source)) {
        for (const key in source) {
            if (isObject(source[key])) {
                if (!target[key]) Object.assign(target, { [key]: {} });
                mergeDeep(target[key], source[key]);
            } else {
                Object.assign(target, { [key]: source[key] });
            }
        }
    }

    return mergeDeep(target, ...sources);
}


export function humanizeByteSize(bytes: number): string {
    if (bytes === 0) return '0 Bytes';

    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const i = Math.floor(Math.log(Math.abs(bytes)) / Math.log(k));

    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

export function bigintToUint8Array (big: bigint): Uint8Array {
    const byteArray = new Uint8Array(8);
    for (let i = 0; i < byteArray.length; i++) {
        byteArray[i] = Number(big >> BigInt(8 * i) & BigInt(0xff));
    }
    return byteArray;
}

import { Writable } from 'stream';

export class MemoryStream extends Writable {
    private buffer: Uint8Array;
    private maxSize: number;
    private currentSize: number;

    constructor(buffer: Buffer, maxSize: number) {
        super();
        this.maxSize = maxSize;
        this.buffer = buffer;
        this.currentSize = 0;
    }

    _write(chunk: Buffer, encoding: string, callback: (error?: Error | null) => void): void {
        if (chunk.length + this.currentSize > this.maxSize) {
            callback(new Error('Buffer overflow'));
            return;
        }

        this.buffer.set(chunk, this.currentSize);
        this.currentSize += chunk.length;
        callback();
    }

    getBufferData(): Buffer {
        return Buffer.from(this.buffer.buffer, this.buffer.byteOffset, this.currentSize);
    }

    clearBuffer(): void {
        this.currentSize = 0;
        // this.buffer.fill(0);
    }
}
