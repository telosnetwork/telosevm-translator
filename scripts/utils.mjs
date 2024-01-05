import {sleep} from "../build/utils/indexer.js";

process.on('SIGINT', () => process.exit(4));
process.on('SIGTERM', () => process.exit(4));
process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    // Print the stack trace if available
    if (reason && reason.stack) {
        console.error(reason.stack);
    }
    // Exit the process with error code 3
    process.exit(3);
});

import path from "path";
import fs, {existsSync, readFileSync} from "fs";

export const SCRIPTS_DIR = path.dirname(fileURLToPath(import.meta.url));
export const TEST_RESOURCES_DIR = path.join(SCRIPTS_DIR, '../build/tests/resources');

const packageJsonFile = path.join(SCRIPTS_DIR, '../package.json');
export const packageInfo = JSON.parse(fs.readFileSync(packageJsonFile, 'utf-8'));

export function makeDirectory(directoryPath) {
    const resolvedPath = path.resolve(directoryPath);

    if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
        console.log(`Directory created: ${resolvedPath}`);
    } else {
        console.log(`Directory already exists: ${resolvedPath}`);
    }
}

import { spawn } from 'child_process';
import {fileURLToPath} from "node:url";


// Function to run a command in bg
export function runCommandInBackground(command, args, cbs) {
    const child = spawn(command, args);

    // Handle standard output data
    child.stdout.on('data', (data) => {if (cbs.message) cbs.message(data);});

    // Handle standard error data
    child.stderr.on('data', (data) => {if (cbs.message) cbs.message(data);});

    // Handle error
    child.on('error', (error) => {
        console.error(`Error on background command!: ${error.message}`);
        console.error(error.stack);
        if (cbs.error)
            cbs.error(error)
    });

    // Handle close
    child.on('close', (code) => {
        if (cbs.close)
            cbs.close(code);
    });

    return child;
}

// Function to run a command and stream its output as utf-8
export async function runCommand(command, args, printFn) {
    const displayMessage = (msg) => {
        printFn(msg.toString().trimEnd());
    };
    return new Promise((resolve, reject) => {
        const child = runCommandInBackground(
            command, args, {message: displayMessage, error: reject, close: resolve}
        );
    });
}

import {Client} from "@elastic/elasticsearch";
import Downloader from "nodejs-file-downloader";
import os from "os";

export async function downloadFile(fileUrl, destinationPath, options) {
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
        onProgress(percentage, chunk, remainingSize) {
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


import zlib from 'zlib';
import lzma from 'lzma-native';
import {ZSTDDecompress} from "simple-zstd";
import { pipeline } from 'stream/promises';
import toxiproxyClient from "toxiproxy-node-client";
import tar from "tar";
import {JsonRpc} from "eosjs";
import {TEVMIndexer} from "../build/indexer.js";
import {assert, expect} from "chai";
import moment from "moment/moment.js";
import {clearInterval} from "timers";
import prettyBytes from "pretty-bytes";
import {StorageEosioActionSchema, StorageEosioDeltaSchema} from "../build/types/evm.js";
import progress from 'progress-stream';


export async function decompressFile(filePath, outputPath) {
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

export async function maybeFetchResource(opts) {
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

// Docker helpers
const DOCKER_PREFIX = `telosevm-translator-${packageInfo.version}`;

export async function buildDocker(name, buildDir) {
    const imageTag = `${DOCKER_PREFIX}:${name}`;
    const buildExitCode = await runCommand(
        'docker',
        ['build', '-t', imageTag, buildDir],
        console.log
    );
    console.log(`Build for ${name} exited with code ${buildExitCode}`);

}

export async function runCustomDocker(image, name, runArgs, cmd) {
    const finalRunArgs = ['run', `--name=${DOCKER_PREFIX}-${name}`, ...runArgs, image];
    if (cmd)
        finalRunArgs.push(cmd);
    const runExitCode = await runCommand('docker', finalRunArgs, console.log);
    console.log(`Run docker ${name} process exited with code ${runExitCode}`);
}

export async function runDocker(name, runArgs, cmd) {
    await runCustomDocker(`${DOCKER_PREFIX}:${name}`, name, runArgs, cmd);
}

export async function stopCustomDocker(name) {
    const stopExitCode = await runCommand(
        'docker', ['stop', name],
        console.log
    );
    console.log(`Stop docker ${name} process exited with code ${stopExitCode}`);
}

export async function stopDocker(name) {
    await stopCustomDocker(`${DOCKER_PREFIX}-${name}`);
}

export async function killDocker(name) {
    const stopExitCode = await runCommand(
        'docker', ['kill', `${DOCKER_PREFIX}-${name}`],
        console.log
    );
    console.log(`Kill docker ${name} process exited with code ${stopExitCode}`);
}

export async function launchNodeos(dataSource, snapshotName, logFile, options) {
    let nodeosRootDir;
    if (options.nodeosRootDir)
        nodeosRootDir = options.nodeosRootDir;
    else
        nodeosRootDir = path.join(os.tmpdir(), 'nodeos');

    if (fs.existsSync(nodeosRootDir))
        throw new Error(`${nodeosRootDir} already exists.`)

    makeDirectory(nodeosRootDir);

    const dataSourcePath = path.join(TEST_RESOURCES_DIR, dataSource);
    if (!fs.existsSync(dataSourcePath))
        throw new Error(`${dataSource} not found at ${dataSourcePath} !`);

    console.log(`Copying ${dataSource} to ${nodeosRootDir}`);
    fs.cpSync(dataSourcePath, nodeosRootDir, {recursive: true});

    const hostSnapshotPath = path.join(TEST_RESOURCES_DIR, snapshotName);
    const guestSnapshotPath = path.join(nodeosRootDir, snapshotName);
    console.log(`Copying ${hostSnapshotPath} to ${guestSnapshotPath}`);
    fs.cpSync(hostSnapshotPath, guestSnapshotPath);

    const nodeosCmd = [
        'nodeos',
            '--config-dir=/root/target',
            '--data-dir=/root/target/data',
            '--disable-replay-opts',
            `--snapshot=/root/target/${snapshotName}`
    ];

    if (options.replay)
        nodeosCmd.push('--replay-blockchain');

    nodeosCmd.push(
        '>>',
        `/root/target/${logFile}`,
        '2>&1'
    );

    const containerCmd = ['/bin/bash', '-c', nodeosCmd.join(' ')];

    console.log(`Launching nodeos with cmd:\n${nodeosCmd}`)
    const launchExitCode = await runCommand(
        'docker',
        [
            'run',
            '-d',  // detach
            '--rm',  // remove after use
            '--network=host',
            '--mount', `type=bind,source=${nodeosRootDir},target=/root/target`,
            `--name=telosevm-translator-${packageInfo.version}-nodeos`,
            'guilledk/py-leap:leap-subst-4.0.4-1.0.1',
            ...containerCmd
        ], console.log
    );
    if (launchExitCode !== 0)
        throw new Error(`Nodeos launch ended with non zero exit code ${launchExitCode}`);
}

export async function initializeNodeos(nodeosOptions) {
    let isNodeosUp = false;
    const runtimeDir = path.join(os.tmpdir(), nodeosOptions.runtimeDir);
    const logFile = 'nodeos.log';
    const logfileHostPath = path.join(runtimeDir, logFile);
    await launchNodeos(
        nodeosOptions.dataDir,
        nodeosOptions.snapshot,
        logFile,
        {nodeosRootDir: runtimeDir, replay: nodeosOptions.replay}
    );
    const nodeosTail = runCommandInBackground(
        'tail', ['-f', logfileHostPath],
        {
            message: (msg) => {
                const msgStr = msg.toString().trimEnd();
                if (msgStr.includes('acceptor_.listen()'))
                    isNodeosUp = true;
                console.log('nodeos-replay: ' + msg.toString().trimEnd())
            },
            error: (error) => {
                console.error(`Nodeos log tail process error: ${error.message}`);
                console.error(error.stack);
                throw error;
            },
            close: (code) => {
                if (code !== 0)
                    console.error(`Nodeos log tail process closed with non zero exit code: ${code}`);
                else
                    console.log('Nodeos log tail process closed with exit code 0');
            }
        }
    )
    while (!isNodeosUp) await sleep(1000);
    nodeosTail.kill('SIGTERM');
}

// elasticdump helpers
export async function maybeLoadElasticDump(dumpName, esConfig) {
    const dumpPath = path.join(TEST_RESOURCES_DIR, dumpName);
    if (!existsSync(dumpPath))
        throw new Error(`elasticdump directory not found at ${dumpPath}`);

    const manifestPath = path.join(dumpPath, 'manifest.json');
    if (!existsSync(manifestPath))
        throw new Error(`elasticdump manifest not found at ${manifestPath}`);

    const es = new Client({node: esConfig.host});

    const manifest = JSON.parse(readFileSync(manifestPath).toString());
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
            for (const index in Object.keys(manifest)) {
                try {
                    await es.indices.delete({index});
                } catch (e) {
                    console.error(`Error while deleting ${index}...`);
                    console.error(e);
                }
            }

        } catch (e) {
            if (!e.message.includes('index_not_found_exception'))
                throw e;
        }

        await es.indices.create({index: indexName});

        const mappingArgs = [`--input=${mappingPath}`, `--output=${esConfig.host}/${indexName}`, '--type=mapping'];
        await runCommand('elasticdump', mappingArgs, (msg) => {console.log(`elasticdump: ${msg}`)});

        const dataArgs = [
            `--input=${dataPath}`, `--output=${esConfig.host}/${indexName}`, '--type=data', `--limit=${esConfig.esDumpLimit}`
        ];
        await runCommand('elasticdump', dataArgs, (msg) => {console.log(`elasticdump: ${msg}`)});
        console.log(`Elastic dump ${dumpName} loaded successfully!`);
    }
}

export async function getElasticDeltas(esclient, index, from, to) {
    try {
        const hits = [];
        let result = await esclient.search({
            index: index,
            query: {
                range: {
                    block_num: {
                        gte: from,
                        lte: to
                    }
                }
            },
            size: 1000,
            sort: [{ 'block_num': 'asc' }],
            scroll: '10s'
        });

        let scrollId = result._scroll_id;
        while (result.hits.hits.length) {
            result.hits.hits.forEach(hit => hits.push(hit._source));
            result = await esclient.scroll({
                scroll_id: scrollId,
                scroll: '10s'
            });
            scrollId = result._scroll_id;
        }

        return hits;
    } catch (e) {
        console.error(e.message);
        console.error(e.stack);
        return [];
    }
}

export async function getElasticActions(esclient, index, from, to) {
    try {
        const hits = [];
        let result = await esclient.search({
            index: index,
            query: {
                range: {
                    '@raw.block': {
                        gte: from,
                        lte: to
                    }
                }
            },
            size: 1000,
            sort: [{ '@raw.block': 'asc' }, { '@raw.trx_index': 'asc' }],
            scroll: '1m' // Keep the search context alive for 1 minute
        });

        let scrollId = result._scroll_id;
        while (result.hits.hits.length) {
            result.hits.hits.forEach(hit => hits.push(hit._source));
            result = await esclient.scroll({
                scroll_id: scrollId,
                scroll: '10s'
            });
            scrollId = result._scroll_id;
        }

        return hits;
    } catch (e) {
        console.error(e.message);
        console.error(e.stack);
        return [];
    }
}

export function chance(percent) {
    return Math.random() <= percent;
}

// toxiproxy-node-client is :(
export async function createShortLivedToxic(proxy, config, ms) {
    const toxic = new toxiproxyClient.Toxic(null, config);
    await proxy.addToxic(toxic);  // circular json error if toxic.proxy != null when doing addToxic
    await sleep(ms);
    toxic.proxy = proxy;
    await toxic.remove();
}


const latencyConfig = {
    name: 'latencyToxic',
    type: 'latency',
    attributes: {
        latency: 0,
        jitter: 500
    }
};

const timeoutConfig = {
    name: 'timeoutToxic',
    type: 'timeout',
    attributes: {
        timeout: 3000
    }
};

const resetPeerConfig = {
    name: 'resetToxic',
    type: 'reset_peer',
    attributes: {
        timeout: 0
    }
};

export async function toxiInitializeTestProxy(
    toxiClient, name, port, toxiPort,
    toxicConfig
) {
    const toxicProxyInfo = {
        name,
        listen: `127.0.0.1:${toxiPort}`,
        upstream: `127.0.0.1:${port}`
    };

    try {
        await(await toxiClient.get(name)).remove();

    } catch (e) {
        if (!('response' in e) ||
            !('data' in e.response) ||
            !('error' in e.response.data) ||
            e.response.data.error !== 'proxy not found')
            throw e;

    }

    const proxy = await toxiClient.createProxy(toxicProxyInfo);

    if (toxicConfig.latency) {
        const toxicBody = {...latencyConfig, attributes: {...toxicConfig.latency}};
        await proxy.addToxic(new toxiproxyClient.Toxic(proxy, toxicBody));
    }

    return proxy;
}

// interface ESVerificationTestParameters {
//     title: string;
//     resources: {
//         type: string;
//         url: string;
//         destinationPath: string;
//         decompressPath?: string;
//     }[];
//     elastic: {
//         host: string;
//         verification: {
//             delta: string;
//             action: string;
//         };
//         esDumpLimit?: number;
//     };
//     toxi?: {
//         toxics: {
//             drops: boolean;
//             latency?: ICreateToxicBody<Latency>;
//         };
//         host: string;
//     };
//     nodeos: {
//         httpPort: number;
//         toxiHttpPort: number;
//         shipPort: number;
//         toxiShipPort: number;
//     }
//     translator: {
//         template: string;
//         startBlock: number;
//         totalBlocks: number;
//         evmPrevHash: string;
//         stallCounter: number;
//     }
// }

export async function translatorESVerificationTest(
    testParams
) {
    const testTitle = testParams.title;
    const testTimeout = testParams.timeout;
    const testTitleSane = testTitle.split(' ').join('-').toLowerCase();
    const testStartTime = new Date().getTime();

    const defESConfig = {
        host: 'http://127.0.0.1:9200',
        verification: {
            delta: '',
            action: ''
        },
        esDumpLimit: 4000
    }
    const esConfig = {...defESConfig, ...testParams.elastic};
    const esClient = new Client({node: esConfig.host});
    try {
        await esClient.ping();
    } catch (e) {
        console.error('Could not ping elastic, is it up?');
        process.exit(1);
    }

    const defToxiConfig = {
        host: 'http://127.0.0.1:8474',
    };
    let toxiConfig = {};
    let toxiProxy;
    if (testParams.toxi) {
        toxiConfig = {...defToxiConfig, ...testParams.toxi};
        toxiProxy = new toxiproxyClient.Toxiproxy(toxiConfig.host);
        try {
            await toxiProxy.getVersion();
        } catch (e) {
            console.error('Could not get toxiproxy version, is it up?');
            process.exit(1);
        }
        await toxiProxy.reset();
    }

    const defNodeosConfig = {
        httpPort: 8888, toxiHttpPort: 8889,
        shipPort: 29999, toxiShipPort: 30000,
    };
    const nodeosConfig = {...defNodeosConfig, ...testParams.nodeos};
    const nodeosHttpHost = `http://127.0.0.1:${nodeosConfig.httpPort}`;
    const nodeosShipHost = `http://127.0.0.1:${nodeosConfig.shipPort}`;
    const toxiNodeosHttpHost = `http://127.0.0.1:${nodeosConfig.toxiHttpPort}`;
    const toxiNodeosShipHost = `http://127.0.0.1:${nodeosConfig.toxiShipPort}`;

    const defTranslatorConfig = {
        template: 'config.mainnet.json',
        startBlock: 180698860,
        totalBlocks: 10000,
        evmPrevHash: '',
        stallCounter: 2
    };
    const translatorTestConfig = {...defTranslatorConfig, ...testParams.translator};
    const templatesDirPath = path.join(SCRIPTS_DIR, '../config-templates');
    const translatorConfig = JSON.parse(
        readFileSync(path.join(templatesDirPath, translatorTestConfig.template)).toString()
    );

    const startBlock = translatorTestConfig.startBlock;
    const endBlock = startBlock + translatorTestConfig.totalBlocks;

    translatorConfig.endpoint = toxiConfig.toxics ? toxiNodeosHttpHost : nodeosHttpHost;
    translatorConfig.wsEndpoint = toxiConfig.toxics ? toxiNodeosShipHost : nodeosShipHost;
    translatorConfig.startBlock = startBlock;
    translatorConfig.stopBlock = endBlock;
    translatorConfig.evmPrevHash = translatorTestConfig.evmPrevHash;
    translatorConfig.perf.stallCounter = translatorTestConfig.stallCounter;

    if (translatorTestConfig.totalBlocks < translatorConfig.perf.elasticDumpSize)
        translatorConfig.perf.elasticDumpSize = translatorTestConfig.totalBlocks;

    const adjustedNum = Math.floor(startBlock / translatorConfig.elastic.docsPerIndex);
    const numericIndexSuffix = String(adjustedNum).padStart(8, '0');
    const genDeltaIndexName = `${translatorConfig.chainName}-${translatorConfig.elastic.subfix.delta}-${numericIndexSuffix}`;
    const genActionIndexName = `${translatorConfig.chainName}-${translatorConfig.elastic.subfix.transaction}-${numericIndexSuffix}`;

    // try to delete generated data in case it exists
    try {
        await esClient.indices.delete({index: genDeltaIndexName});
    } catch (e) {
    }
    try {
        await esClient.indices.delete({index: genActionIndexName});
    } catch (e) {
    }

    // maybe download & decompress resources
    let nodeosSnapshotName = undefined;
    let nodeosSnapshotPath = undefined;
    const maybeInitializeResouce = async (res) => {
        const resName = await maybeFetchResource(res)
        if (res.type === 'esdump')
            await maybeLoadElasticDump(resName, esConfig);

        else if (res.type === 'snapshot') {
            nodeosSnapshotName = resName;
            nodeosSnapshotPath = res.decompressPath;

        } else if (res.type === 'nodeos') {
            if (!nodeosSnapshotName) {
                console.log('Nodeos launch pending until snapshot available...');
                while(!nodeosSnapshotName) await sleep(1000);
            }
            console.log(`Snapshot ${nodeosSnapshotName} found, maybe launching nodeos...`);

            try {
                const nodeosRpc = new JsonRpc(nodeosHttpHost);
                const chainInfo = await nodeosRpc.get_info();

                if (chainInfo.earliest_available_block_num > startBlock)
                    throw new Error(`Nodeos does not contain ${startBlock}`);
                if (chainInfo.head_block_num < endBlock)
                    throw new Error(`Nodeos does not contain ${endBlock}`);

                console.log(`Nodeos contains start & end block, skipping replay...`);

            } catch (e) {
                if (!e.message.includes('Could not find block') &&
                    !(e.message.includes('fetch failed')))
                    throw e;

                await initializeNodeos({
                    snapshot: nodeosSnapshotName,
                    dataDir: resName, runtimeDir: testTitleSane,
                    replay: !!res.replay
                });
            }
        }
    };
    if (testParams.resources)
        await Promise.all(testParams.resources.map(
            res => maybeInitializeResouce(res)
        ));

    let httpProxy = undefined;
    let shipProxy = undefined;
    let dropTask;
    let totalTimeouts = 0;
    let totalResets = 0;
    let totalDowntime = 0;
    if (toxiConfig.toxics) {
        httpProxy = await toxiInitializeTestProxy(
            toxiProxy, `${testTitleSane}-nodeos-http`,
            testParams.nodeos.httpPort,
            testParams.nodeos.toxiHttpPort,
            toxiConfig.toxics
        );
        shipProxy = await toxiInitializeTestProxy(
            toxiProxy, `${testTitleSane}-nodeos-ship`,
            testParams.nodeos.shipPort,
            testParams.nodeos.toxiShipPort,
            toxiConfig.toxics
        );

        if ('drops' in toxiConfig.toxics &&
            toxiConfig.toxics.drops) {
            let isDropTaskInprogress = false;
            setTimeout(() => {
                dropTask = setInterval(async () => {
                    if (!isDropTaskInprogress) {
                        isDropTaskInprogress = true;
                        const dropStartTime = new Date().getTime();

                        if (chance(1 / 20)) {
                            await createShortLivedToxic(httpProxy, resetPeerConfig, 50);
                            await createShortLivedToxic(shipProxy, resetPeerConfig, 50);
                            totalResets++;
                        } else {
                            if (chance(1 / 60)) {
                                await createShortLivedToxic(httpProxy, timeoutConfig, 6000);
                                await createShortLivedToxic(shipProxy, timeoutConfig, 6000);
                                totalTimeouts++;
                            }
                        }

                        const dropTimeElapsed = (new Date().getTime()) - dropStartTime;
                        totalDowntime += dropTimeElapsed;
                        isDropTaskInprogress = false;
                    }
                }, 1000);
            }, 1000);
        }
    }

    // launch translator and verify generated data
    let isTranslatorDone = false;
    const translator = new TEVMIndexer(translatorConfig);

    let esQueriesDone = 0;
    let esDocumentsChecked = 0;

    const translatorLaunchTime = new Date().getTime();
    let translatorSyncTime;
    await translator.launch();
    translator.connector.events.on('write', async (writeInfo) => {
        // get all block documents in write range
        const deltas = await getElasticDeltas(esClient, genDeltaIndexName, writeInfo.from, writeInfo.to);
        const verifiedDeltas = await getElasticDeltas(esClient, esConfig.verification.delta, writeInfo.from, writeInfo.to);
        assert(deltas.length === verifiedDeltas.length, 'getElasticDelta length difference');

        deltas.forEach((delta, index) => {
            const verifiedDelta = StorageEosioDeltaSchema.parse(verifiedDeltas[index]);
            const parsedDelta = StorageEosioDeltaSchema.parse(delta);

            expect(parsedDelta).to.be.deep.equal(verifiedDelta);
            esDocumentsChecked++;
        });

        // get all tx documents in write range
        const actions = await getElasticActions(esClient, genActionIndexName, writeInfo.from, writeInfo.to);
        esQueriesDone++;
        const verifiedActions = await getElasticActions(esClient, esConfig.verification.action, writeInfo.from, writeInfo.to);
        esQueriesDone++;
        assert(actions.length === verifiedActions.length, 'getElasticActions length difference');

        actions.forEach((action, index) => {
            const verifiedActionDoc = verifiedActions[index];
            const verifiedAction = StorageEosioActionSchema.parse(verifiedActionDoc);
            const parsedAction = StorageEosioActionSchema.parse(action);

            expect(parsedAction).to.be.deep.equal(verifiedAction);
            esDocumentsChecked++;
        });

        const now = new Date().getTime();

        const currentTestTimeElapsed = moment.duration(now - testStartTime, 'ms').humanize();
        const currentSyncTimeElapsed = moment.duration(now - translatorLaunchTime, 'ms').humanize();

        const checkedBlocksCount = writeInfo.to - startBlock;
        const progressPercent = (((checkedBlocksCount / translatorTestConfig.totalBlocks) * 100).toFixed(2) + '%').padStart(6, ' ');
        const currentProgress = (writeInfo.to - startBlock).toLocaleString();

        console.log('-'.repeat(32));
        console.log('Test stats:');
        console.log(`last checked range ${writeInfo.from.toLocaleString()} - ${writeInfo.to.toLocaleString()}`);
        console.log(`progress: ${progressPercent}, ${currentProgress} blocks`);
        console.log(`total test time: ${currentTestTimeElapsed}`);
        console.log(`total sync time: ${currentSyncTimeElapsed}`);
        console.log(`es queries done: ${esQueriesDone}`);
        console.log(`es docs checked: ${esDocumentsChecked}`);
        console.log('-'.repeat(32));

        if (writeInfo.to === endBlock) {
            isTranslatorDone = true;
            translatorSyncTime = new Date().getTime();
        }
    });

    while (!isTranslatorDone) {
        const now = new Date().getTime();
        if (((now - translatorSyncTime) / 1000) >= testTimeout)
            throw new Error('Translator sync timeout exceeded!');
        await sleep(200);
    }

    const totalTestTimeElapsed = translatorSyncTime - testStartTime;
    const translatorSyncTimeElapsed = translatorSyncTime - translatorLaunchTime;

    const posibleSyncTimeSeconds = (translatorSyncTimeElapsed - totalDowntime) / 1000;

    console.log('Test passed!');
    console.log(`total time elapsed: ${moment.duration(totalTestTimeElapsed, 'ms').humanize()}`);
    console.log(`sync time:          ${moment.duration(translatorSyncTimeElapsed, 'ms').humanize()}`);
    console.log(`down time:          ${moment.duration(totalDowntime, 'ms').humanize()}`);
    console.log(`es queries done:    ${esQueriesDone}`);
    console.log(`es docs checked:    ${esDocumentsChecked}`);
    if (toxiConfig.toxics) {
        if (toxiConfig.toxics.drops) {
            console.log(`# of reset_peer:    ${totalResets}`);
            console.log(`# of timeout:       ${totalTimeouts}`);
        }
        if (toxiConfig.toxics.latency) {
            console.log(`applied latency:    ${latencyConfig.attributes.latency}ms`);
            console.log(`applied jitter:     ${latencyConfig.attributes.jitter}ms`);
        }
    }
    console.log(`average speed:      ${(translatorTestConfig.totalBlocks / posibleSyncTimeSeconds).toFixed(2).toLocaleString()}`);

    if (dropTask)
        clearInterval(dropTask);


    if (toxiConfig.toxics) {
        if (httpProxy)
            await httpProxy.remove();

        if (shipProxy)
            await shipProxy.remove();
    }

    await esClient.close();
}