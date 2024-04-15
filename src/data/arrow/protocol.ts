import {bigintToUint8Array} from "../../utils/misc.js";
import {promises as fs} from "fs";
import memfs from "node:fs";
import {tableFromIPC} from "apache-arrow";
import {ZSTDDecompress} from 'simple-zstd';
import RLP from "rlp";

export enum ArrowBatchCompression {
    UNCOMPRESSED = 0,
    ZSTD = 1
}

export interface ArrowBatchGlobalHeader {
    versionConstant: string;
}

export interface ArrowBatchHeader {
    headerConstant: string;
    batchByteSize: bigint;
    compression: ArrowBatchCompression;
}

export interface ArrowBatchFileMetadata {
    header: ArrowBatchGlobalHeader,
    batches: {batch: ArrowBatchHeader, start: number, end: number}[]
}

export class ArrowBatchProtocol {
    /*
     * arrow-batch spec
     *
     * we need a binary format that allows us streaming new rows to a file
     * in a way the files can be arbitrarily large, but still retain fast
     * random access properties.
     *
     * arrow-batch format gives us this by sequentially appending random access
     * arrow tables of a specific batch size plus a small header before each,
     * to form a bigger table.
     *
     * file map:
     *
     *     global header: version constant + batch size (uint64) + total batch count (uint64)
     *
     *     batch #0 header: batch header constant + batch byte size (uint64) + compression (uint8)
     *     arrow random access file bytes...
     *
     *     batch #1 header
     *     arrow random access file bytes...
     *
     * constants:
     *     version constant: ascii ord `ARROW-BATCH1`
     *     compression enum:
     *         0 - uncompressed
     *         1 - zstd  # TODO: impl
     *
     * # streaming:
     * this structure can be streamed easily by reading each batch header as it comes in then
     * expect to read the new full arrow random access batch sent.
     *
     * # random access:
     * to do a random access on a disk arrow-batch file, first one would read the global header
     * then before reading any actual arrow table data, read all arrow batch headers in order by
     * seeking around on the file by the specified metadata values on the batch headers, once the
     * batch that contains the row we are looking for is reached we can read that batch and do
     * queries that only affect that small batch.
     */
    static readonly ARROW_BATCH_VERSION_CONSTANT = 'ARROW-BATCH1';
    static readonly GLOBAL_HEADER_SIZE = ArrowBatchProtocol.ARROW_BATCH_VERSION_CONSTANT.length;

    static readonly ARROW_BATCH_HEADER_CONSTANT = 'ARROW-BATCH-TABLE';
    static readonly BATCH_HEADER_SIZE = ArrowBatchProtocol.ARROW_BATCH_HEADER_CONSTANT.length + 8 + 1;

    static newGlobalHeader(): Uint8Array {
        return new TextEncoder().encode(
            ArrowBatchProtocol.ARROW_BATCH_VERSION_CONSTANT);
    }

    static newBatchHeader(byteSize: bigint, compression: ArrowBatchCompression) {
        const strBytes = new TextEncoder().encode(
            ArrowBatchProtocol.ARROW_BATCH_HEADER_CONSTANT);

        const batchSizeBytes = bigintToUint8Array(byteSize);
        const compressionByte = new Uint8Array([compression]);

        const buffer = new Uint8Array(strBytes.length + batchSizeBytes.length + 1);
        buffer.set(strBytes, 0);
        buffer.set(batchSizeBytes, strBytes.length);
        buffer.set(compressionByte, strBytes.length + batchSizeBytes.length);

        return buffer;
    }

    static readGlobalHeader(buffer: Buffer): ArrowBatchGlobalHeader {
        const versionConstantLength = this.ARROW_BATCH_VERSION_CONSTANT.length;
        const versionConstantBytes = buffer.subarray(0, versionConstantLength);
        const versionConstant = new TextDecoder("utf-8").decode(versionConstantBytes);

        return { versionConstant };
    }

    static readBatchHeader(buffer: Buffer): ArrowBatchHeader {
        const headerConstantLength = this.ARROW_BATCH_HEADER_CONSTANT.length;
        const headerConstantBytes = buffer.subarray(0, headerConstantLength);
        const headerConstant = new TextDecoder("utf-8").decode(headerConstantBytes);

        const sizeStart = headerConstantLength;
        const batchByteSize = buffer.readBigUInt64LE(sizeStart);
        const compression = buffer.readUint8(sizeStart + 8);

        return { headerConstant, batchByteSize, compression };
    }

    static async readFileMetadata(filePath: string): Promise<ArrowBatchFileMetadata> {
        const fileHandle = await fs.open(filePath, 'r');
        const fileStat = await fileHandle.stat();

        const globalHeaderBuff = Buffer.alloc(ArrowBatchProtocol.GLOBAL_HEADER_SIZE);
        const batchHeaderBuff = Buffer.alloc(ArrowBatchProtocol.BATCH_HEADER_SIZE);

        let offset = 0;
        await fileHandle.read(globalHeaderBuff, 0, globalHeaderBuff.length, offset);
        offset += ArrowBatchProtocol.GLOBAL_HEADER_SIZE;
        const globalHeader = ArrowBatchProtocol.readGlobalHeader(globalHeaderBuff);

        const metadata = {
            header: globalHeader,
            batches: []
        }

        while (offset < fileStat.size) {
            await fileHandle.read(batchHeaderBuff, 0, batchHeaderBuff.length, offset);
            const batch = ArrowBatchProtocol.readBatchHeader(batchHeaderBuff);
            const batchSize = parseInt(batch.batchByteSize.toString(), 10);
            offset += ArrowBatchProtocol.BATCH_HEADER_SIZE;
            metadata.batches.push(
                {batch, start: offset, end: offset + batchSize - 1});
            offset += parseInt(batch.batchByteSize.toString(), 10);
        }

        await fileHandle.close();

        return metadata;
    }

    static async readArrowBatchTable(filePath: string, metadata: ArrowBatchFileMetadata, batchIndex: number) {
        const batchMeta = metadata.batches[batchIndex];
        const readStream = memfs.createReadStream(filePath, {start: batchMeta.start, end: batchMeta.end});
        let finalStream = readStream;
        switch (batchMeta.batch.compression) {
            case ArrowBatchCompression.UNCOMPRESSED: {
                break;
            }
            case ArrowBatchCompression.ZSTD: {
                finalStream = ZSTDDecompress()
                // @ts-ignore
                readStream.pipe(finalStream)
                break;
            }
        }
        const serializedTable = await new Promise<Uint8Array>((resolve, reject) => {
            const chunks = [];
            finalStream.on('data', (chunk: Buffer) => {
                chunks.push(chunk);
                if (chunk.slice(chunk.length - 'ARROW1'.length).toString() === 'ARROW1')
                    resolve(new Uint8Array(Buffer.concat(chunks)));
            });
            finalStream.on('error', reject);
        });
        const table = tableFromIPC(serializedTable);
        return table;
    }
}

export type ArrowUnsignedIntType = 'u8' | 'u16' | 'u32' | 'u64' | 'uintvar';
export type ArrowIntType = 'i64';
export type ArrowNumberType = ArrowUnsignedIntType | ArrowIntType;
export type ArrowByteFieldType = 'string' | 'bytes' | 'base64';
export type ArrowDigestType = 'checksum160' | 'checksum256';
export interface ArrowTableMapping {
    name: string;
    type: ArrowNumberType | ArrowByteFieldType | ArrowDigestType;

    optional?: boolean;
    length?: number;
    array?: boolean;
    ref?: {table: string, field: string};
}

const nullForType = {
    u8: 0, u16: 0, u32: 0, u64: BigInt(0), uintvar: BigInt(0),
    i64: BigInt(0),

    string: '',
    bytes: '',
    base64: '',

    checksum160: '00'.repeat(20),
    checksum256: '00'.repeat(32)
}

const arraysPerType = {
    u8: Uint8Array, u16: Uint16Array, u32: Uint32Array, u64: BigUint64Array,
    i64: BigInt64Array
};

export function getArrayFor(fieldInfo: ArrowTableMapping) {
    let arrType = Array;

    if (typeof fieldInfo.type === 'string' &&
        fieldInfo.type in arraysPerType)
        arrType = arraysPerType[fieldInfo.type];

    return arrType;
}


const validationFunctions = {
    u8:  (value: any) => {
        if (typeof value === 'string')
            value = parseInt(value);

        return typeof value === 'number' && value < (2 ** 8)
    },
    u16: (value: any) => {
        if (typeof value === 'string')
            value = parseInt(value);
        return typeof value === 'number' && value < (2 ** 16)
    },
    u32: (value: any) => {
        if (typeof value === 'string')
            value = parseInt(value);
        return typeof value === 'number' && value < (2 ** 32)
    },
    u64: (value: any) => {
        if (['bigint', 'boolean', 'number', 'string'].includes(typeof value)) {
            const num = BigInt(value);
            return num < (BigInt(2) ** BigInt(64));
        }
        return false;
    },
    uintvar: (value: any) => {
        return ['bigint', 'number', 'string'].includes(typeof value) ||
            value instanceof Uint8Array ||
            value instanceof Buffer;
    },

    i64: (value: any) => {
        if (['bigint', 'boolean', 'number', 'string'].includes(typeof value)) {
            const num = BigInt(value);
            const limit = BigInt(2) ** BigInt(63);
            return num > -limit && num < limit;

        }
        return false;
    },

    bytes:  (value: any) => {
        return typeof value === 'string' ||
            value instanceof Uint8Array ||
            value instanceof Buffer;
    },
    string: (value: any) => typeof value === 'string',
    checksum160: (value: any) => {
        return typeof value === 'string' ||
            value instanceof Uint8Array ||
            value instanceof Buffer;
    },
    checksum256: (value: any) => {
        return typeof value === 'string' ||
            value instanceof Uint8Array ||
            value instanceof Buffer;
    }
}

const encodeFunctions = {
    u8: (value: any, fieldInfo: ArrowTableMapping) => {
        if (typeof value === 'string')
            value = parseInt(value);
        return value;
    },
    u16: (value: any, fieldInfo: ArrowTableMapping) => {
        if (typeof value === 'string')
            value = parseInt(value);
        return value;
    },
    u32: (value: any, fieldInfo: ArrowTableMapping) => {
        if (typeof value === 'string')
            value = parseInt(value);
        return value;
    },
    u64: (value: any, fieldInfo: ArrowTableMapping) => BigInt(value),
    uintvar: (value: any, fieldInfo: ArrowTableMapping) => {
        let typedValue: Buffer;
        if (['bigint', 'number', 'string'].includes(typeof value)) {
            let hex;
            try {
                value = BigInt(value);
                hex = value.toString(16);
            } catch (e) {
                hex = value;
            }

            if (hex.length % 2 !== 0)
                hex = '0' + hex;

            const byteLength = hex.length / 2;
            const bytes = new Uint8Array(byteLength);

            for (let i = 0, j = 0; i < byteLength; ++i, j += 2)
                bytes[i] = parseInt(hex.slice(j, j + 2), 16);

            typedValue = Buffer.from(bytes);

        } else if (value instanceof Uint8Array)
            typedValue = Buffer.from(value);

        return typedValue.toString('base64');
    },

    i64: (value: any, fieldInfo: ArrowTableMapping) => BigInt(value),

    bytes: (value: any, fieldInfo: ArrowTableMapping) => {
        let typedValue: Buffer;
        let valByteLength = value.length;
        if (typeof value === 'string') {
            if (value.startsWith('0x'))
                value = value.substring(2);
            typedValue = Buffer.from(value, 'hex');
            valByteLength /= 2;

        } else if (value instanceof Uint8Array)
            typedValue = Buffer.from(value);

        if (fieldInfo.length && valByteLength != fieldInfo.length)
            throw new Error(
                `Invalid row byte field length for ${fieldInfo.name}, value length ${value.length} but expected ${fieldInfo.length}`);

        return typedValue.toString('base64');
    },
    string: (value: any, fieldInfo: ArrowTableMapping) => value,

    checksum160: (value: any, fieldInfo: ArrowTableMapping) => {
        let typedValue: Buffer;
        if (typeof value === 'string') {
            if (value.startsWith('0x'))
                value = value.substring(2);
            typedValue = Buffer.from(value.padStart('0', 40), 'hex');

        } else if (value instanceof Uint8Array)
            typedValue = Buffer.from(value);

        if (typedValue.length > 20)
            throw new Error(
                `Invalid row byte field length for ${fieldInfo.name}, value length ${typedValue.length} but expected 20`);

        return typedValue.toString('base64');
    },
    checksum256: (value: any, fieldInfo: ArrowTableMapping) => {
        let typedValue: Buffer;
        if (typeof value === 'string') {
            if (value.startsWith('0x'))
                value = value.substring(2);
            typedValue = Buffer.from(value.padStart('0', 64), 'hex');

        } else if (value instanceof Uint8Array)
            typedValue = Buffer.from(value);

        if (typedValue.length > 32)
            throw new Error(
                `Invalid row byte field length for ${fieldInfo.name}, value length ${typedValue.length} but expected 32`);

        return typedValue.toString('base64');
    }
};

export function encodeRowValue(tableName: string, fieldInfo: ArrowTableMapping, value: any) {
    const fieldType = fieldInfo.type;

    // handle optionals
    if (fieldInfo.optional && !value) {
        if (fieldInfo.array)
            value = [];
        else
            value = nullForType[fieldType];
    }

    // handle normal values
    if (fieldInfo.array && Array.isArray(value)) {
        // const typedValueArray = value.map(
        //     internalVal =>  encodeRowValue(tableName, fieldInfo, internalVal));

        try {
            let rlpEncodedArray = RLP.encode(value);
            return Buffer.from(rlpEncodedArray).toString('base64');
        } catch (e) {
            throw e;
        }
    }

    // validate
    if (!(fieldType in validationFunctions))
        throw new Error(`No encode validation function for ${fieldType}`);

    const validationFn = validationFunctions[fieldType];

    if (!validationFn(value))
        throw new Error(
            `Invalid row field value at ${tableName}.${fieldInfo.name}, can\'t cast value ${value} to ${fieldInfo.type}`);

    const encodeFn = encodeFunctions[fieldType];
    return encodeFn(value, fieldInfo);
}

const decodeFunctions = {
    u8: (value: any) => value,
    u16: (value: any) => value,
    u32: (value: any) => value,
    u64: (value: any) => BigInt(value.toString()),
    uintvar: (bytes: string) => {
        const hex = Buffer.from(bytes, 'base64').toString('hex');
        return BigInt('0x' + hex).toString();
    },

    i64: (value: any) => value.toString(),

    bytes: (bytes: string) => {
        return Buffer.from(bytes, 'base64').toString('hex');
    },
    string: (value: any) => value,

    checksum160: (bytes: string) => {
        return Buffer.from(bytes, 'base64').toString('hex');
    },
    checksum256: (bytes: string) => {
        return Buffer.from(bytes, 'base64').toString('hex');
    }
};

export function decodeRowValue(tableName: string, fieldInfo: ArrowTableMapping, value: any) {
    const fieldType = fieldInfo.type;

    // handle optionals
    if (fieldInfo.optional && !value) {
        if (fieldInfo.array)
            value = [];
        else
            value = nullForType[fieldType];
    }

    // handle normal values
    if (fieldInfo.array) {
        const fieldValues = RLP.decode(Buffer.from(value, 'base64'));
        return fieldValues.map(
            internalVal => decodeRowValue(tableName, fieldInfo, internalVal));
    }

    if (!(fieldType in validationFunctions))
        throw new Error(`No encode validation function for ${fieldType}`);

    // decode
    const decodeFn = decodeFunctions[fieldType];
    const decodedValue = decodeFn(value);

    // validate
    const validationFn = validationFunctions[fieldType];

    if (!validationFn(decodedValue))
        throw new Error(
            `Invalid row field value at ${tableName}.${fieldInfo.name}, can\'t cast value ${value} to ${fieldInfo.type}`);

    return decodedValue;
}
