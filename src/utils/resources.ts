import path from "path";
import fs, {existsSync, readFileSync} from "fs";
import {pipeline} from "stream/promises";
import progress from 'progress-stream';

import Downloader from "nodejs-file-downloader";
import prettyBytes from "pretty-bytes";
import tar from "tar";
import zlib from "zlib";
import lzma from 'lzma-native';
import {ZSTDDecompress} from "simple-zstd";

import {Client} from "@elastic/elasticsearch";
import {TEST_RESOURCES_DIR} from "./indexer.js";
import {runCommand} from "./docker.js";
import {ElasticConnectorConfig} from "../types/indexer";


export function makeDirectory(directoryPath: string) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    }
}

export async function downloadFile(
    fileUrl: string,
    destinationPath: string,
    options: {force?: boolean, quiet?: boolean}
) {
    if (existsSync(destinationPath) && !options.force) {
        console.log(`${destinationPath} exists and --force not passed, skipping...`);
        return;
    }

    const fileName = path.basename(destinationPath);
    const reportedFor = [];
    const downloadStartTime = Date.now(); // Start time of the download
    let downloadedSoFar = 0; // Total downloaded bytes so far

    const downloader = new Downloader({
        url: fileUrl,
        directory: path.dirname(destinationPath),
        fileName,
        cloneFiles: false,
        onProgress(percentage, chunk: Uint8Array, remainingSize) {
            downloadedSoFar += chunk.length; // Update total downloaded bytes
            const currentTime = Date.now();
            const totalElapsedTime = (currentTime - downloadStartTime) / 1000; // Total time in seconds since the download started

            let averageSpeed = 0;
            if (totalElapsedTime > 0) {
                averageSpeed = downloadedSoFar / totalElapsedTime; // Average speed in bytes per second
            }

            const formattedSpeed = prettyBytes(averageSpeed) + '/s';
            const numericPercent = parseFloat(percentage);
            const mod = options.quiet ? 5 : 1;

            if (!reportedFor.includes(numericPercent) &&
                numericPercent % mod === 0) {
                console.log(`${fileName}: ${(percentage + '%').padEnd(7, ' ')} | Downloaded: ${prettyBytes(downloadedSoFar)} | Remaining: ${prettyBytes(remainingSize)} | Avg Speed: ${formattedSpeed}`);
                reportedFor.push(numericPercent);
            }
        }
    });
    try {
        console.log(`Downloading: ${fileUrl}`);
        const downloadResult = await downloader.download();
        console.log('Download succeded!')
        console.log(downloadResult);

        console.log(`Downloaded resource to ${downloadResult.filePath}`);

    } catch (e) {
        console.error('Download failed:');
        throw e;
    }
}

export async function decompressFile(filePath: string, outputPath: string) {
    if (existsSync(outputPath)) {
        console.log(`Skipping decompress cause ${outputPath} exists...`);
        return;
    }

    const fileExtension = filePath.split('.').pop();
    console.log(`Decompressing ${fileExtension} file to ${outputPath}...`);

    let decompressPipe;
    switch (fileExtension) {
        case 'gz':
            // Using zlib for .gz files
            decompressPipe = zlib.createGunzip();
            break;

        case 'xz':
            // Using lzma-native for .xz files
            decompressPipe = lzma.createDecompressor();
            break;

        case 'zst':
        case 'zstd':
            // Using node-zstandard for .zstd or .zst files
            decompressPipe = ZSTDDecompress();
            break;

        default:
            throw new Error(`Unsupported compression format ${fileExtension}`);
    }
    let pipeOutputPath = outputPath;
    let isTar = filePath.includes('.tar');
    if (isTar)
        pipeOutputPath += '.tar';

    const reportedFor = [];
    const stat = fs.statSync(filePath);
    const decompressProgStream = progress({
        length: stat.size,
        time: 1000 /* Update interval in milliseconds */
    });
    decompressProgStream.on('progress', (stats) => {
        const percent = stats.percentage.toFixed(2);
        const numericPercent = parseFloat(percent);
        const formattedSpeed = prettyBytes(stats.speed) + '/s';
        if (!reportedFor.includes(numericPercent)) {
            console.log(`Decompress: ${pipeOutputPath}: ${(percent + '%').padEnd(7, ' ')} | Speed: ${formattedSpeed}`);
            reportedFor.push(numericPercent);
        }
    });

    await pipeline(
        fs.createReadStream(filePath),
        decompressProgStream,
        decompressPipe,
        fs.createWriteStream(pipeOutputPath)
    );

    if (isTar) {
        console.log(`Untar file ${pipeOutputPath}...`);

        const extractStream = tar.extract({
            cwd: path.dirname(outputPath),
            onentry: (entry) => {
                console.log(`Extracting: ${entry.path}`);
            }
        });

        const tarSize = fs.statSync(pipeOutputPath).size;
        const tarProgressStream = progress({
            length: tarSize,
            time: 100 // Update interval in milliseconds
        });

        reportedFor.length = 0;
        tarProgressStream.on('progress', (stats) => {
            const percent = stats.percentage.toFixed(2);
            const numericPercent = parseFloat(percent);
            const formattedSpeed = prettyBytes(stats.speed) + '/s';
            if (!reportedFor.includes(numericPercent)) {
                console.log(`Untar: ${pipeOutputPath}: ${(percent + '%').padEnd(7, ' ')} | Speed: ${formattedSpeed}`);
                reportedFor.push(numericPercent);
            }
        });

        await pipeline(
            fs.createReadStream(pipeOutputPath),
            tarProgressStream,
            extractStream
        );

        fs.unlinkSync(pipeOutputPath);
        console.log(`Untar complete.`);
    }
    console.log(`Decompressed ${outputPath}`);
}

export async function maybeFetchResource(opts: {url: string, decompressPath: string, destinationPath: string}) {
    const destinationPath = path.join(TEST_RESOURCES_DIR, opts.destinationPath);
    await downloadFile(opts.url, destinationPath, {quiet: true});
    let finalName = opts.destinationPath;
    if (opts.decompressPath) {
        const decompressPath = path.join(TEST_RESOURCES_DIR, opts.decompressPath);
        await decompressFile(destinationPath, decompressPath);
        finalName = opts.decompressPath;
    }
    return finalName;
}

export interface ESDumpManifestEntry {
    mapping: string;
    data: string;
    size: number;
}

// elasticdump helpers
export async function maybeLoadElasticDump(
    dumpName: string,
    esConfig: ElasticConnectorConfig
) {
    const dumpPath = path.join(TEST_RESOURCES_DIR, dumpName);
    if (!existsSync(dumpPath))
        throw new Error(`elasticdump directory not found at ${dumpPath}`);

    const manifestPath = path.join(dumpPath, 'manifest.json');
    if (!existsSync(manifestPath))
        throw new Error(`elasticdump manifest not found at ${manifestPath}`);

    const es = new Client(esConfig);

    const manifest = JSON.parse(readFileSync(manifestPath).toString()) as {[key: string]: ESDumpManifestEntry};
    let mustDelete = false;
    for (const [indexName, indexInfo] of Object.entries(manifest)) {
        const mappingPath = path.join(dumpPath, indexInfo.mapping);
        const dataPath = path.join(dumpPath, indexInfo.data);
        if ((!existsSync(mappingPath)) || (!existsSync(dataPath)))
            throw new Error(`index file not found ${mappingPath} or ${dataPath}`);

        // expect index_not_found_exception
        try {
            const indexDocCount = await es.count({index: indexName});
            if (indexDocCount.count === indexInfo.size) {
                console.log(`skipping ${indexName} as its already present and doc amount matches...`);
                continue;
            }
            console.log(
                `${indexName} exists but its doc count (${indexDocCount.count}) doesnt match manifest: ${indexInfo.size} ` +
                `delete and re-import...`
            );
            try {
                await es.indices.delete({index: indexName});
            } catch (e) {
                console.error(`Error while deleting ${indexName}...`);
                console.error(e);
            }

        } catch (e) {
            if (!e.message.includes('index_not_found_exception'))
                throw e;
        }

        await es.indices.create({index: indexName});

        const mappingArgs = [`--input=${mappingPath}`, `--output=${esConfig.node}/${indexName}`, '--type=mapping'];
        await runCommand('elasticdump', mappingArgs, (msg) => {console.log(`elasticdump: ${msg}`)});

        const dataArgs = [
            `--input=${dataPath}`, `--output=${esConfig.node}/${indexName}`, '--type=data', `--limit=4000`
        ];
        await runCommand('elasticdump', dataArgs, (msg) => {console.log(`elasticdump: ${msg}`)});
        console.log(`Elastic dump ${dumpName} loaded successfully!`);
    }
}
