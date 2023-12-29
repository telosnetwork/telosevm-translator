import {IndexerState, IndexerConfig, IndexedBlockInfo} from "../types/indexer.js";
import {TEVMIndexer} from "../indexer.js";
import {Connector} from "../database/connector.js";

import {assert, expect} from "chai";
import {sleep, logger, ControllerContext, NewChainInfo, ChainRuntime, getRandomPort, ControllerConfig} from "leap-mock";
import {describe} from "mocha";


export async function expectTranslatorSequence(
    translatorConfig: IndexerConfig,
    blockSequence: number[]
) {
    const connector = new Connector(translatorConfig, logger);
    await connector.init();
    await connector.purgeNewerThan(1);
    await connector.deinit();

    const translator = new TEVMIndexer(translatorConfig);
    translator.state = IndexerState.HEAD;

    let i = 0;
    let reachedEnd = false;
    let isExpectedSequence = true;
    const receivedSequence = [];
    translator.events.on('push-block', async (block) => {
        if (reachedEnd)
            return;

        const currentBlock: number = block.delta.block_num;

        // const currentHash: string = block.delta['@blockHash'];
        // const currentEVMBlock: number = block.delta['@global'].block_num;
        // const currentEVMHash: string = block.delta['@evmBlockHash'];
        // const currentEVMPrevHash: string = block.delta['@evmPrevBlockHash'];

        receivedSequence.push(currentBlock);

        isExpectedSequence = isExpectedSequence && (currentBlock == blockSequence[i]);
        i++;
        reachedEnd = i == blockSequence.length;
    });

    await translator.launch();

    while(isExpectedSequence && !reachedEnd)
        await sleep(500);

    await translator.stop();

    return assert.deepStrictEqual(
        receivedSequence, blockSequence, 'Received wrong sequence from ship');
}

import {portFromEndpoint} from "../utils/misc.js";
import {ActionDescriptor} from "leap-mock/build/types.js";
import {
    StorageEosioAction,
    StorageEosioActionSchema, StorageEosioDelta,
    StorageEosioDeltaSchema, StorageEosioGenesisDelta, StorageEosioGenesisDeltaSchema
} from "../types/evm.js";
import {Client} from "@elastic/elasticsearch";
import cloneDeep from "lodash.clonedeep";
import {arrayToHex, removeHexPrefix, ZERO_ADDR} from "../utils/evm.js";
import {Bloom} from "@ethereumjs/vm";

export interface TestContext {
    ctx: ControllerContext;
    chainInfo: NewChainInfo;
    runtime: ChainRuntime;
    elastic: {
        genesis: StorageEosioGenesisDelta,
        actions: StorageEosioAction[],
        deltas: StorageEosioDelta[]
    }
}

export function describeMockChainTests(
    title: string,
    translatorConfig: IndexerConfig,
    tests: {
        [key: string]: {
            sequence: number[],
            chainConfig: {
                shipPort?: number,
                httpPort?: number,
                blocks?: string[][],
                jumps?: [number, number][],
                pauses?: [number, number][],
                txs?: {[key: number]: ActionDescriptor[]}
            },
            testFn?: (testCtx: TestContext) => Promise<void>
        }
    }
) {
    describe(title, async function() {
        const config: ControllerConfig = {controlPort: await getRandomPort()};
        const context = new ControllerContext(config);

        const shipPort = portFromEndpoint(translatorConfig.wsEndpoint);
        const httpPort = portFromEndpoint(translatorConfig.endpoint);
        const es = new Client(translatorConfig.elastic);

        const getDocumentsAtIndex = async function(indexName: string, sort: any) {
            let allDocuments: any[] = [];
            let from = 0;
            const size = 1000; // Adjust size as needed

            while (true) {
                const response = await es.search({
                    index: indexName,
                    sort: sort,
                    from: from,
                    size: size
                });

                const hits = response.hits.hits;

                if (hits.length === 0) {
                    break; // Break the loop if no more documents are found
                }

                allDocuments = [...allDocuments, ...hits.map(hit => hit._source)];
                from += size;
            }

            return allDocuments;
        }

        const getActionDocuments = async function() {
            return getDocumentsAtIndex(
                `${translatorConfig.chainName}-${translatorConfig.elastic.subfix.transaction}-*`,
                [{'@timestamp': 'asc'}, {'@raw.block': 'asc'}, {'action_ordinal': 'asc'}]
            );
        }

        const getDeltaDocuments = async function() {
            return getDocumentsAtIndex(
                `${translatorConfig.chainName}-${translatorConfig.elastic.subfix.delta}-*`,
                [{'block_num': 'asc'}]
            );
        }

        before(async function ()  {
            await context.bootstrap();
        });
        beforeEach(async function () {
            await context.startTest(this.currentTest.title);
        });
        afterEach(async function () {
            await context.endTest(this.currentTest.title);
        });
        after(async function () { await context.teardown() });

        for (const testName in tests) {
            const testInfo = tests[testName];
            context.registerTestChain(testName, {...testInfo.chainConfig, shipPort, httpPort});
            it(testName, async function() {
                const chainInfo = context.getTestChain(testName);
                const customConfig = cloneDeep(translatorConfig);
                customConfig.startBlock = Math.min(...testInfo.sequence);
                customConfig.stopBlock = Math.max(...testInfo.sequence);
                await expectTranslatorSequence(
                    customConfig,
                    testInfo.sequence
                );
                const actionDocs = await getActionDocuments();
                const deltaDocs = await getDeltaDocuments();

                // validate docs
                const actions = [];
                for (const action of actionDocs)
                    actions.push(StorageEosioActionSchema.parse(action));

                // first should always be genesis
                const deltas = [];
                const genesis = StorageEosioGenesisDeltaSchema.parse(deltaDocs.shift());
                for (const delta of deltaDocs)
                    deltas.push(StorageEosioDeltaSchema.parse(delta));

                const blockAmount = customConfig.stopBlock - customConfig.startBlock + 1;
                expect(deltas.length).to.be.eq(blockAmount);

                if (testInfo.testFn) {
                    await testInfo.testFn({
                        ctx: context,
                        chainInfo,
                        runtime: context.controller.getRuntime(chainInfo.chainId),
                        elastic: {
                            genesis, deltas, actions
                        }
                    });
                }
            });
        }

    });
}
