import {Readable, Writable} from "stream";

export function bigIntMin(...args: bigint[]) {
    if (args.length === 0) {
        throw new Error('At least one argument is required');
    }

    let min = args[0];
    for (let i = 1; i < args.length; i++) {
        if (args[i] < min) {
            min = args[i];
        }
    }

    return min;
}

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

export function getB64DecodedLength(base64String: string): number {
    let padding = 0;

    if (base64String.endsWith('==')) {
        padding = 2;
    } else if (base64String.endsWith('=')) {
        padding = 1;
    }

    return ((base64String.length / 4) * 3) - padding;
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

import {ZSTDCompress} from 'simple-zstd';
export async function compressUint8Array(input: Uint8Array, compressionLevel = 3) {
    // Convert Uint8Array to a Buffer since Node.js streams work with Buffers
    const inputBuffer = Buffer.from(input);

    // Create a readable stream from the input buffer
    const readableStream = new Readable({
        read() {
            this.push(inputBuffer);
            this.push(null); // Signal end of stream
        }
    });

    // Create a writable stream to collect the output
    const chunks = [];
    const writableStream = new Writable({
        write(chunk, encoding, callback) {
            chunks.push(chunk);
            callback();
        }
    });

    // Pipe the readable stream through the compression stream and into the writable stream
    readableStream
        .pipe(ZSTDCompress(compressionLevel))
        .pipe(writableStream);

    // Wait for the stream to finish
    await finished(writableStream);

    // Combine the chunks into a single Buffer
    const outputBuffer = Buffer.concat(chunks);

    // Convert the output Buffer back to a Uint8Array and return it
    return new Uint8Array(outputBuffer);
}

export class MemoryWriteStream extends Writable {
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

export class MemoryReadStream extends Readable {
    private buffer: Buffer;
    private currentIndex: number;

    constructor(buffer: Buffer, options?: ConstructorParameters<typeof Readable>[0]) {
        super(options);
        this.buffer = buffer;
        this.currentIndex = 0;
    }

    _read(size: number): void {
        if (this.currentIndex < this.buffer.length) {
            const chunk = this.buffer.slice(this.currentIndex, this.currentIndex + size);
            this.currentIndex += chunk.length;
            this.push(chunk);
        } else {
            this.push(null); // Signal the end of the stream
        }
    }
}

import {LogEntry} from "winston";
import Transport from "winston-transport";
import {finished} from "stream/promises";

export interface WorkerLogMessage {
    name: any;
    method: 'workerLog';
    log: LogEntry;
}

export function isWorkerLogMessage(msg: any): msg is WorkerLogMessage {
    return 'name' in msg &&
        'method' in msg && msg.method === 'workerLog' &&
        'log' in msg;
}

export class WorkerTransport extends Transport {

    private readonly postLog: (msg: LogEntry) => void;

    constructor(postLog, opts) {
        super(opts);
        this.postLog = postLog;
    }

    log(info: LogEntry, callback) {
        this.postLog(info);
        callback();
    }
}