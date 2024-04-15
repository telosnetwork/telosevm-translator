import fs from "node:fs";
import path from "node:path";
import {RecordBatch, RecordBatchFileWriter, Table, tableFromArrays} from "apache-arrow";
import {ArrowBatchCompression, ArrowBatchProtocol, ArrowTableMapping, encodeRowValue, getArrayFor} from "./protocol.js";

import { parentPort, workerData } from 'node:worker_threads';
import {format, LogEntry, Logger, loggers} from "winston";
import {compressUint8Array, MemoryWriteStream, WorkerTransport} from "../../utils/misc.js";
import {finished} from "stream/promises";

const DEFAULT_STREAM_BUF_MEM = 30 * 1024 * 1024;
const streamBuffer = Buffer.alloc(DEFAULT_STREAM_BUF_MEM);

export interface WriterControlRequest {
    tid: number
    method: 'addRow' | 'flush'
    params?: any
}

export interface WriterControlResponse {
    tid: number,
    name: string,
    method: string,
    status: 'ok' | 'error',
    error?: any
}

let {
    tableName, alias, tableMappings, compression, logLevel
}: {

    tableName: string,
    alias?: string,
    tableMappings: ArrowTableMapping[],
    compression:  ArrowBatchCompression,

    logLevel: string

} = workerData;

const loggingOptions = {
    exitOnError: false,
    level: logLevel,
    format: format.json(),
    transports: [
        new WorkerTransport(
            (log: LogEntry) => {
                parentPort.postMessage({
                    name: tableName,
                    method: 'workerLog',
                    log
                });
            },
        {})
    ]
}
const logger: Logger = loggers.add(`worker-internal-${tableName}`, loggingOptions);

const intermediateBuffers = {};
let intermediateSize = 0;
function _initBuffer() {
    intermediateSize = 0;
    for (const mapping of tableMappings)
        intermediateBuffers[mapping.name] = [];
    logger.debug(`initialized buffers for ${Object.keys(intermediateBuffers)}`);
}

function _generateArrowBatchTable(): Table {
    const arrays = {};
    for (const mapping of tableMappings)
        arrays[mapping.name] = getArrayFor(mapping).from(intermediateBuffers[mapping.name]);
    try {
        return tableFromArrays(arrays);
    } catch (e) {
        console.log('lol');
        throw e;
    }
}

function _initDiskBuffer(currentFile: string) {
    fs.writeFileSync(
        currentFile,
        ArrowBatchProtocol.newGlobalHeader()
    );
    logger.debug(`wrote global header on ${currentFile}`);
}

async function serializeTable(table: Table): Promise<Uint8Array> {
    const writeStream = new MemoryWriteStream(streamBuffer, DEFAULT_STREAM_BUF_MEM);
    // pipe record batch writer through it
    const blocksWriter = RecordBatchFileWriter.throughNode();
    blocksWriter.pipe(writeStream);
    // write batch, flush buffers
    const batch: RecordBatch = table.batches[0];
    blocksWriter.write(batch);
    blocksWriter.end();
    await finished(writeStream);
    return writeStream.getBufferData();
}

function flush(msg: WriterControlRequest) {
    if (intermediateSize == 0)
        return;

    const fileName = `${alias ?? tableName}.ab${msg.params.unfinished ? '.wip' : ''}`;
    const currentFile = path.join(msg.params.writeDir, fileName);

    logger.debug(`generating arrow table from intermediate...`);
    const startTableGen = performance.now();
    const table = _generateArrowBatchTable();
    logger.debug(`table generated, took: ${performance.now() - startTableGen}`);

    _initBuffer();  // from here on, ready to let the parentPort listener run in bg

    logger.debug(`serializing table...`);
    const startSerialize = performance.now();
    serializeTable(table).then(serializedBatch => {
        logger.debug(`serialized, took: ${performance.now() - startSerialize}`);

        const write = (batchBytes: Uint8Array) => {
            if (!fs.existsSync(currentFile))
                _initDiskBuffer(currentFile);

            // header
            logger.debug(`writing batch to disk...`);
            const startWrite = performance.now();
            fs.appendFileSync(
                currentFile, ArrowBatchProtocol.newBatchHeader(BigInt(batchBytes.length), compression));

            // content
            fs.appendFileSync(currentFile, batchBytes);
            logger.debug(`${batchBytes.length.toLocaleString()} bytes written to disk, took: ${performance.now() - startWrite}`);

            parentPort.postMessage({
                tid: msg.tid,
                name: tableName,
                method: msg.method,
                status: 'ok'
            });
        };

        switch (compression) {
            case ArrowBatchCompression.UNCOMPRESSED: {
                write(serializedBatch);
                break;
            }
            case ArrowBatchCompression.ZSTD: {
                const startCompress = performance.now();
                compressUint8Array(serializedBatch, 10).then(bytes => {
                    logger.debug(`${bytes.length.toLocaleString()} bytes after compression, took: ${performance.now() - startCompress}`);
                    write(bytes);
                });
                break;
            }
        }
    });
}

function addRow(msg: WriterControlRequest) {
    const typedRow = tableMappings.map(
        (fieldInfo, index) => {
            try {
                return encodeRowValue(tableName, fieldInfo, msg.params[index])
            } catch (e) {
                logger.error(`error encoding ${fieldInfo}, ${index}`);
                throw e;
            }
        });

    tableMappings.forEach(
        (fieldInfo, index) => {
            const fieldColumn = intermediateBuffers[fieldInfo.name];
            fieldColumn.push(typedRow[index])
        });

    intermediateSize++;

    parentPort.postMessage({
        tid: msg.tid,
        name: tableName,
        method: msg.method,
        status: 'ok'
    });
}

const handlers = {
  addRow, flush
};

_initBuffer();

parentPort.on('message', (msg: WriterControlRequest) => {
    const resp: WriterControlResponse = {
        tid: msg.tid,
        name: tableName,
        method: msg.method,
        status: 'ok'
    };
    const throwError = e => {
        resp.status = 'error';
        resp.error = e;
        parentPort.postMessage(resp);
    };
    try {
        if (!(msg.method in handlers))
            throwError(new Error(`Unknown method \'${msg.method}\'!`));

        handlers[msg.method](msg);
    } catch (e) {
        throwError(e);
    }
})