import {runCommand, runCommandInBackground, SCRIPTS_DIR, TEST_RESOURCES_DIR} from "../utils.mjs";
import path from "path";
import {assert} from "chai";
import {existsSync, rmdirSync} from "fs";
import {sleep} from "../../build/utils/indexer.js";

/*
 * Welcome to the 1 Millon block verification test harness
 *
 * This test requires the following services running:
 *
 *     - elasticsearch @ 9200
 *     - toxiproxy @ 8474
 *
 * This test downloads the following files if not present already
 * (default location: build/tests/resources):
 *
 *     - telos-mainnet v6 snapshot from 2022-05-29 around block 218 million
 *     - block data & config files to start up a node with state for block range 218-219 million
 *     - generated es data from delta & actions indexes of a correct run
 */
const elasticHost = 'http://127.0.0.1:9200';

// STEP 1: Maybe download & decompress resources
const nodeosDataName = 'nodeos-mainnet-218m-219m';
const nodeosResourcesPath = path.join(TEST_RESOURCES_DIR, nodeosDataName);

const snapshotUrl = 'https://ops.store.eosnation.io/telos-snapshots/snapshot-2022-05-29-14-telos-v6-0217795040.bin.zst';
const snapshotFilePath = path.join(nodeosResourcesPath, 'snapshot-2022-05-29-14-telos-v6-0217795040.bin');
const snapshotCompressedFilePath = path.join(nodeosResourcesPath, `${snapshotFilePath}.zst`);
const snapDownloadCode = await runCommand(
    'node',
    [`${SCRIPTS_DIR}/tests/downloadFile.mjs`, snapshotUrl, snapshotCompressedFilePath, '-d'],
    console.log
);

assert(snapDownloadCode == 0, 'Snapshot download failed!');

// TODO: download block data
const nodeosDataFile = `${nodeosDataName}.tar.zst`;
// const nodeosDataUrl = `https://storage.telos.net/${nodeosDataFile}`

// TODO: download elastic data
const esDumpName = 'telos-mainnet-v1.5-218m-219m-elasticdump';

// STEP 2: launch elasticdump bg import process
let isDumpDone = false;
const loadDumpScript = path.join(SCRIPTS_DIR, 'tests/loadElasticDump.mjs')
runCommandInBackground(
    'node', [loadDumpScript, esDumpName, elasticHost],
    {
        message: (msg) => {console.log('es-dump: ' + msg.toString().trimEnd())},
        error: (error) => {
            console.error(`ES Import process error: ${error.message}`);
            console.error(error.stack);
            throw error;
        },
        close: (code) => {
            if (code == 0)
                isDumpDone = true;
            else
                console.error(`ES Import process closed with non zero exit code: ${code}`);
        }
    }
);

// STEP 3: start nodeos replay
let isReplayDone = false;

const nodeosRootDataPath = path.join(SCRIPTS_DIR, '../nodeos-1mil');
const nodeosLogfilePath = path.join(nodeosRootDataPath, 'nodeos.log');

// delete node tmp dir if exists
if (existsSync(nodeosRootDataPath)) {
    console.log(`${nodeosRootDataPath} exists, deleting and re-creating from sources...`);
    rmdirSync(nodeosRootDataPath, {recursive: true});
}

const launchNodeosScript = path.join(SCRIPTS_DIR, 'tests/launchNodeos.mjs')
await runCommand(
    'node', [
        launchNodeosScript, nodeosDataName, snapshotFilePath,
        `--nodeosRootDir=${nodeosRootDataPath}`
    ],
    (msg) => {console.log('nodeos-launch: ' + msg)}
);
runCommandInBackground(
    'tail', ['-f', nodeosLogfilePath],
    {
        message: (msg) => {console.log('nodeos-replay' + msg.toString().trimEnd())},
        error: (error) => {
            console.error(`Nodeos log tail process error: ${error.message}`);
            console.error(error.stack);
            throw error;
        },
        close: (code) => {
            if (code == 0)
                 isReplayDone = true;
            else
                console.error(`Nodeos log tail process closed with non zero exit code: ${code}`);
        }
    }
)

// STEP 4: wait for when reindex & elasticdump loading are done
while (!isDumpDone && !isReplayDone)
    await sleep(200);

// STEP 5: configure toxiproxy

// STEP 6: launch translator and reindex, perform verification on the fly