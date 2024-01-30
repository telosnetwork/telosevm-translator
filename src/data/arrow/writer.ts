import workerpool from 'workerpool';
import {writeFileSync} from "fs";


function writeBucket(
    bucket: Uint8Array,
    dstPath: string
) {
    const start = performance.now();
    console.log(`${dstPath} writer received ${bucket.length} bytes`);

    writeFileSync(dstPath, bucket);
    console.log(`wrote ${dstPath} took ${performance.now() - start} ms`);
}

workerpool.worker({
    writeBucket
});