import {IndexerConfig, IndexerState} from "../types/indexer.js";
import {TEVMIndexer} from "../indexer.js";
import {Connector} from "../database/connector.js";

import {assert} from "chai";
import {sleep, logger} from "leap-mock";


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