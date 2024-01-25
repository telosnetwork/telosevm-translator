import {IndexerState, TranslatorConfig} from "../types/indexer.js";
import {TEVMIndexer} from "../indexer.js";
import {BlockData, Connector} from "../data/connector.js";

import {assert, expect} from "chai";
import {sleep, logger, ControllerContext, NewChainInfo, ChainRuntime, getRandomPort, ControllerConfig} from "leap-mock";
import {describe} from "mocha";


export async function expectTranslatorSequence(
    translatorConfig: TranslatorConfig,
    blockSequence: number[]
) {
    const translator = new TEVMIndexer(translatorConfig);
    translator.state = IndexerState.HEAD;

    await translator.targetConnector.init();
    await translator.targetConnector.purgeNewerThan(1);
    await translator.targetConnector.deinit();

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
    blocks: BlockData[];
}

export function describeMockChainTests(
    title: string,
    translatorConfig: TranslatorConfig,
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

        const shipPort = portFromEndpoint(translatorConfig.source.nodeos.wsEndpoint);
        const httpPort = portFromEndpoint(translatorConfig.source.nodeos.endpoint);

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
                const minBlock = Math.min(...testInfo.sequence);
                const maxBlock = Math.max(...testInfo.sequence);
                customConfig.target.chain = {};
                customConfig.target.chain.startBlock = minBlock;
                customConfig.target.chain.stopBlock = maxBlock;
                await expectTranslatorSequence(
                    customConfig,
                    testInfo.sequence
                );
                const translator = new TEVMIndexer(customConfig);
                await translator.targetConnector.init();
                const blocks = await translator.targetConnector.getBlockRange(minBlock, maxBlock);

                const blockAmount = maxBlock - minBlock + 1;
                expect(blocks.length).to.be.eq(blockAmount);

                if (testInfo.testFn) {
                    await testInfo.testFn({
                        ctx: context,
                        chainInfo,
                        runtime: context.controller.getRuntime(chainInfo.chainId),
                        blocks
                    });
                }
            });
        }

    });
}
