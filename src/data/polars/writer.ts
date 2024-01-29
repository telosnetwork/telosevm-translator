import pl, {DataType} from 'nodejs-polars';
import workerpool from 'workerpool';
import {_columnarObj, unpackRow} from "./polars.js";


function _unwrapRow(row: any[], columns: Record<string, any[]>, schema: Record<string, DataType>) {
    Object.keys(schema).forEach((columnName, index) => {
        columns[columnName].push(row[index]);
    });
}

function writeData(
    data: SharedArrayBuffer,
    rowsAmount: number,
    frameOptions: any,
    dstPath: string,
    format: "parquet" | "ipc" | "csv",
    writeOptions: any
) {
    const rows = new Uint8Array(data);

    const start = performance.now();
    console.log(`${dstPath} writer received ${rows.length} bytes`);

    const columns = _columnarObj(frameOptions.schema);

    console.log(`${dstPath} writer stacking ${rowsAmount} rows...`);
    for (let r = 0; r < rowsAmount; r++)
        _unwrapRow(unpackRow(rows, r), columns, frameOptions.schema);

    const df = pl.DataFrame(columns, frameOptions);
    console.log(`${dstPath} writer loaded rows into df.`);

    switch (format) {
        case "parquet": {
            df.writeParquet(dstPath, writeOptions);
            break;
        }
        case "ipc": {
            df.writeIPC(dstPath, writeOptions);
            break;
        }
        case "csv": {
            df.writeCSV(dstPath, writeOptions);
            break;
        }
    }
    console.log(`took ${performance.now() - start} ms`);
}

workerpool.worker({
    writeData
});