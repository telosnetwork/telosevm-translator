import {translatorESReindexVerificationTest} from "../utils.mjs";
import {Command} from "commander";


/*
 * Reindex a 100k block range 312mil blocks in, this range was selected due to transaction density.
 */
const program = new Command();
program
    .option('-b, --totalBlocks [totalBlocks]', 'Set a specific amount of blocks to sync')
    .option('-L, --limit [limit]', 'Number of documents per elasticdump import batch')
    .action(async (options) => {
        await translatorESReindexVerificationTest({
            title: '100k late range',
            timeout: 20 * 60,  // 20 minutes in seconds
            esDumpName: 'telos-mainnet-318-100k',
            elastic: {
                host: 'http://127.0.0.1:9200',
                esDumpLimit: options.limit ? parseInt(options.limit, 10) : 4000
            },
            srcPrefix: 'telos-mainnet-verification',
            dstPrefix: 'telos-mainnet-reindex',
            indexVersion: 'v1.5',
            template: 'config.mainnet.json',
            startBlock: 312087081,
            totalBlocks: options.totalBlocks ? parseInt(options.totalBlocks, 10) : 100000 - 1,
            evmPrevHash: '',
            evmValidateHash: '',
            esDumpSize: 10000,
            scrollSize: 10000,
            scrollWindow: '1m'
        });
    });

program.parse(process.argv);

import {Client} from "@elastic/elasticsearch";
import path from "path";
import {readFileSync} from "fs";
import {sleep} from "../../build/utils/indexer.js";
import {TEVMIndexer} from "../../build/indexer.js";
import {assert, expect} from "chai";
import {StorageEosioActionSchema, StorageEosioDeltaSchema} from "../../build/types/evm.js";
import {
    decompressFile,
    getElasticActions, getElasticDeltas, maybeLoadElasticDump,
    SCRIPTS_DIR, TEST_RESOURCES_DIR,
} from "../utils.mjs";
import moment from "moment";

const testStartTime = new Date().getTime();
const esConfig = {
    host: 'http://127.0.0.1:9200',
    esDumpLimit: 4000,
    verification: {
        delta: 'telos-mainnet-verification-delta-v1.5-00000000',
        action: 'telos-mainnet-verification-action-v1.5-00000000'
    }
}
const esClient = new Client({node: esConfig.host});
try {
    await esClient.ping();
} catch (e) {
    console.error('Could not ping elastic, is it up?');
    process.exit(1);
}

const translatorTestConfig = {
    template: 'config.mainnet.json',
    startBlock: 36,
    endBlock: 99965,
    evmPrevHash: '',
};
const templatesDirPath = path.join(SCRIPTS_DIR, '../config-templates');
const translatorConfig = JSON.parse(
    readFileSync(path.join(templatesDirPath, translatorTestConfig.template)).toString()
);

const startBlock = translatorTestConfig.startBlock;
const endBlock = translatorTestConfig.endBlock;

translatorConfig.chainName = 'telos-mainnet-verification';
translatorConfig.startBlock = startBlock;
translatorConfig.stopBlock = endBlock;
translatorConfig.evmPrevHash = translatorTestConfig.evmPrevHash;
// translatorConfig.evmValidateHash = '13f28fe4d164354cbcb0b9d8d43dff5d8e4b180e440579a55505a5fc96831c6b';

if (translatorTestConfig.totalBlocks < translatorConfig.perf.elasticDumpSize)
    translatorConfig.perf.elasticDumpSize = translatorTestConfig.totalBlocks;

const adjustedNum = Math.floor(startBlock / translatorConfig.elastic.docsPerIndex);
const numericIndexSuffix = String(adjustedNum).padStart(8, '0');
const reindexPrefix = 'telos-mainnet-reindex';
const reindexDeltaIndexName = `telos-mainnet-reindex-${translatorConfig.elastic.subfix.delta}-${numericIndexSuffix}`;
const reindexActionIndexName = `telos-mainnet-reindex-${translatorConfig.elastic.subfix.transaction}-${numericIndexSuffix}`;

// try to delete generated data in case it exists
try {
    await esClient.indices.delete({index: reindexDeltaIndexName});
} catch (e) {
}
try {
    await esClient.indices.delete({index:  reindexActionIndexName});
} catch (e) {
}

const esDumpName = 'telos-mainnet-200k-ebr';
const esDumpCompressedPath = path.join(TEST_RESOURCES_DIR, `${esDumpName}.tar.zst`);
const esDumpPath = path.join(TEST_RESOURCES_DIR, esDumpName);
await decompressFile(esDumpCompressedPath, esDumpPath);
await maybeLoadElasticDump('telos-mainnet-200k-ebr', esConfig);


// launch translator and verify generated data
let isTranslatorDone = false;
const translator = new TEVMIndexer(translatorConfig);

let esQueriesDone = 0;
let esDocumentsChecked = 0;

const translatorLaunchTime = new Date().getTime();
let translatorReindexTime;
await translator.reindex(reindexPrefix);
translator.connector.events.on('write', async (writeInfo) => {
    // get all block documents in write range
    const deltas = await getElasticDeltas(esClient, reindexDeltaIndexName, writeInfo.from, writeInfo.to);
    const verifiedDeltas = await getElasticDeltas(esClient, esConfig.verification.delta, writeInfo.from, writeInfo.to);
    assert(deltas.length === verifiedDeltas.length, 'getElasticDelta length difference');

    deltas.forEach((delta, index) => {
        const verifiedDelta = StorageEosioDeltaSchema.parse(verifiedDeltas[index]);
        const parsedDelta = StorageEosioDeltaSchema.parse(delta);

        expect(parsedDelta).to.be.deep.equal(verifiedDelta);
        esDocumentsChecked++;
    });

    // get all tx documents in write range
    const actions = await getElasticActions(esClient,  reindexActionIndexName, writeInfo.from, writeInfo.to);
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
    const currentReindexTimeElapsed = moment.duration(now - translatorLaunchTime, 'ms').humanize();

    const checkedBlocksCount = writeInfo.to - startBlock;
    const progressPercent = (((checkedBlocksCount / translatorTestConfig.totalBlocks) * 100).toFixed(2) + '%').padStart(6, ' ');
    const currentProgress = (writeInfo.to - startBlock).toLocaleString();

    console.log('-'.repeat(32));
    console.log('Test stats:');
    console.log(`last checked range ${writeInfo.from.toLocaleString()} - ${writeInfo.to.toLocaleString()}`);
    console.log(`progress: ${progressPercent}, ${currentProgress} blocks`);
    console.log(`total test time:    ${currentTestTimeElapsed}`);
    console.log(`total reindex time: ${currentReindexTimeElapsed}`);
    console.log(`es queries done: ${esQueriesDone}`);
    console.log(`es docs checked: ${esDocumentsChecked}`);
    console.log('-'.repeat(32));

    if (writeInfo.to === endBlock) {
        isTranslatorDone = true;
        translatorReindexTime = new Date().getTime();
    }
});

while (!isTranslatorDone)
    await sleep(200);

const totalTestTimeElapsed = translatorReindexTime - testStartTime;
const translatorReindexTimeElapsed = translatorReindexTime - translatorLaunchTime;

console.log('Test passed!');
console.log(`total time elapsed: ${moment.duration(totalTestTimeElapsed, 'ms').humanize()}`);
console.log(`sync time:          ${moment.duration(translatorReindexTimeElapsed, 'ms').humanize()}`);
console.log(`es queries done:    ${esQueriesDone}`);
console.log(`es docs checked:    ${esDocumentsChecked}`);

await esClient.close();