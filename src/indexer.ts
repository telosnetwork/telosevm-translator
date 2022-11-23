import StateHistoryBlockReader from './ship';


import {
    IndexedBlockInfo,
    IndexerConfig,
    IndexerState,
    StartBlockInfo
} from './types/indexer';

import logger from './utils/winston';

import {StorageEosioAction} from './types/evm';

import {Connector} from './database/connector';

import {
    BlockHeader,
    formatBlockNumbers,
    generateBloom,
    generateReceiptRootHash,
    generateTxRootHash,
    getBlockGas,
    ProcessedBlock,
    StorageEosioDelta
} from './utils/evm'

import BN from 'bn.js';
import moment from 'moment';
import PriorityQueue from 'js-priority-queue';


const sleep = (ms: number) => new Promise( res => setTimeout(res, ms));


process.on('unhandledRejection', error => {
    logger.error('Unhandled Rejection');
    logger.error(JSON.stringify(error, null, 4));
    // @ts-ignore
    logger.error(error.message);
    // @ts-ignore
    logger.error(error.stack);
    process.exit(1);
});

export class TEVMIndexer {
    endpoint: string;  // nodeos http rpc endpoint
    wsEndpoint: string;  // nodoes ship ws endpoint

    evmDeployBlock: number;  // native block number where telos.evm was deployed
    startBlock: number;  // native block number to start indexer from as defined by env vars or config
    stopBlock: number;  // native block number to stop indexer from as defined by env vars or config
    ethGenesisHash: string;  // calculated ethereum genesis hash

    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap
    switchingState: boolean = false;  // flag required to do state switching cleanly

    config: IndexerConfig;  // global indexer config as defined by envoinrment or config file

    private reader: StateHistoryBlockReader;  // websocket state history connector, deserializes nodeos protocol
    connector: Connector;  // custom elastic search db driver

    private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    lastOrderedBlock: number;  // last native block number that was succesfully pushed to db in order
    lastNativeOrderedBlock: number;  // last evm block number that was succesfully pushed to db in order

    private blocksQueue: PriorityQueue<ProcessedBlock> = new PriorityQueue({
        comparator: function(a, b) {
            return a.evmBlockNumber - b.evmBlockNumber;
        }
    });  // queue of blocks pending for processing

    private ordering: boolean = false;  // flag required to limit the amount of ordering tasks to one at all times
    private forked: boolean = false  // flag required to limit the amount of fork handling tasks to one at all times

    // debug status used to print statistics
    private queuedUpLastSecond: number = 0;
    private pushedLastSecond: number = 0;
    private idleWorkers: number = 0;

    constructor(telosConfig: IndexerConfig) {
        this.config = telosConfig;

        this.endpoint = telosConfig.endpoint;
        this.wsEndpoint = telosConfig.wsEndpoint;

        this.evmDeployBlock = telosConfig.evmDeployBlock;

        this.startBlock = telosConfig.startBlock;
        this.stopBlock = telosConfig.stopBlock;

        this.connector = new Connector(telosConfig);

        this.reader = new StateHistoryBlockReader(
            this, {
            min_block_confirmation: 1,
            ds_threads: telosConfig.perf.workerAmount,
            allow_empty_deltas: true,
            allow_empty_traces: true,
            allow_empty_blocks: true,
            delta_whitelist: ['contract_row', 'contract_table']
        });
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    updateDebugStats() {
        logger.debug(`Last second ${this.queuedUpLastSecond} blocks were queued up.`);
        let statsString = `${formatBlockNumbers(this.lastNativeOrderedBlock, this.lastOrderedBlock)} pushed, at ${this.pushedLastSecond} blocks/sec` +
            ` ${this.idleWorkers}/${this.config.perf.concurrencyAmount} workers idle`;
        const untilHead = this.reader.headBlock - this.reader.currentBlock;

        if (untilHead > 3) {
            const hoursETA = `${((untilHead / this.pushedLastSecond) / (60 * 60)).toFixed(1)}hs`;
            statsString += `, ${untilHead} to reach head, aprox ${hoursETA}`;
        }

        logger.info(statsString);
        this.queuedUpLastSecond = 0;
        this.pushedLastSecond = 0;
    }

    /*
     * Generate valid ethereum has, requires blocks to be passed in order, updates state
     * handling class attributes.
     */
    hashBlock(block: ProcessedBlock) {
        const evmTxs = block.evmTxs;

        // generate valid ethereum hashes
        const transactionsRoot = generateTxRootHash(evmTxs);
        const receiptsRoot = generateReceiptRootHash(evmTxs);
        const bloom = generateBloom(evmTxs);

        const {gasUsed, gasLimit} = getBlockGas(evmTxs);

        const blockTimestamp = moment.utc(block.blockTimestamp);

        // generate 'valid' block header
        const blockHeader = BlockHeader.fromHeaderData({
            'parentHash': Buffer.from(this.prevHash, 'hex'),
            'transactionsTrie': transactionsRoot,
            'receiptTrie': receiptsRoot,
            'bloom': bloom,
            'number': new BN(block.evmBlockNumber),
            'gasLimit': gasLimit,
            'gasUsed': gasUsed,
            'difficulty': new BN(0),
            'timestamp': new BN(blockTimestamp.unix()),
            'extraData': Buffer.from(block.nativeBlockHash, 'hex')
        })

        const currentBlockHash = blockHeader.hash().toString('hex');

        // generate storeable block info
        const storableActions: StorageEosioAction[] = [];
        const storableBlockInfo: IndexedBlockInfo = {
            "transactions": storableActions,
            "errors": block.errors,
            "delta": new StorageEosioDelta({
                "@timestamp": blockTimestamp.format(),
                "block_num": block.nativeBlockNumber,
                "code": "eosio",
                "table": "global",
                "@global": {
                    "block_num": block.evmBlockNumber
                },
                "@evmBlockHash": currentBlockHash,
                "@receiptsRootHash": receiptsRoot.toString('hex'),
                "@transactionsRoot": transactionsRoot.toString('hex'),
                "gasUsed": gasUsed.toString('hex'),
                "gasLimit": gasLimit.toString('hex')
            }),
            "nativeHash": block.nativeBlockHash.toLowerCase(),
            "parentHash": this.prevHash,
            "receiptsRoot": receiptsRoot.toString('hex'),
            "blockBloom": bloom.toString('hex')
        };

        if (evmTxs.length > 0) {
            for (const [i, evmTxData] of evmTxs.entries()) {
                evmTxData.evmTx.block_hash = currentBlockHash;
                delete evmTxData.evmTx['raw'];
                storableActions.push({
                    "@timestamp": block.blockTimestamp,
                    "trx_id": evmTxData.trx_id,
                    "action_ordinal": evmTxData.action_ordinal,
                    "signatures": evmTxData.signatures,
                    "@raw": evmTxData.evmTx
                });
            }
        }

        this.prevHash = currentBlockHash;

        return storableBlockInfo;
    }

    /*
     * Return newest block from priority queue or null in case queue is empty
     */
    maybeGetNewestBlock() {
        try {
            return this.blocksQueue.peek();
        } catch(e) {
            logger.debug(`getNewestBlock called but queue is empty!`);
            return null;
        }
    }

    /*
     * Orderer routine, gets periodically called to drain blocks priority queue,
     * manages internal state class attributes to guarantee only one task consumes
     * from queue at a time.
     */
    async orderer() {
        // make sure we have blocks we need to order, no other orderer task
        // is running
        if (this.ordering || this.blocksQueue.length == 0)
            return;

        logger.debug('Running orderer...');
        let newestBlock: ProcessedBlock = this.maybeGetNewestBlock();

        if (newestBlock == null) {
            this.ordering = false;
            return;
        }

        this.ordering = true;

        const firstBlockNum = newestBlock.evmBlockNumber;
        const firstNativeBlockNum = newestBlock.nativeBlockNumber;
        logger.debug(`Peek result ${newestBlock.toString()}`);
        logger.debug(`Looking for ${formatBlockNumbers(this.lastNativeOrderedBlock + 1, this.lastOrderedBlock + 1)}...`);

        await this.maybeHandleFork(newestBlock);

        // While blocks queue is not empty, and contains next block we are looking for loop
        while(newestBlock != null && newestBlock.evmBlockNumber == this.lastOrderedBlock + 1) {

            const storableBlockInfo = this.hashBlock(newestBlock);

            // Push to db
            await this.connector.pushBlock(storableBlockInfo);

            if (this.blocksQueue.length == 0)  // Sanity check
                throw new Error(`About to call dequeue with blocksQueue.length == 0!`);

            // Remove newest block of queue
            this.blocksQueue.dequeue();

            // Update block num state tracking attributes
            this.lastOrderedBlock = newestBlock.evmBlockNumber;
            this.lastNativeOrderedBlock = newestBlock.nativeBlockNumber;

            // For debug stats
            this.pushedLastSecond++;

            // Awknowledge block
            this.reader.finishBlock();

            // Step machinery
            newestBlock = this.maybeGetNewestBlock();
            if (newestBlock != null)
                await this.maybeHandleFork(newestBlock);
        }

        // Debug push statistics
        const blocksPushed = this.lastOrderedBlock - firstBlockNum;
        if (blocksPushed > 0) {
            logger.debug(`pushed  ${blocksPushed} blocks, range: ${
                formatBlockNumbers(firstNativeBlockNum, firstBlockNum)}-${
                    formatBlockNumbers(this.lastNativeOrderedBlock, this.lastOrderedBlock)}`);
        }

        // Clear ordering flag allowing new ordering tasks to start
        this.ordering = false;
    }

    /*
     * State history on-block-deserialized call back, pushes blocks out of order
     * will sleep if block received is too far from last stored block.
     */
    async consumer(block: ProcessedBlock): Promise<void> {

        this.blocksQueue.queue(block);
        this.queuedUpLastSecond++;

        if (this.state == IndexerState.HEAD)
            return;

        // worker catch up machinery
        while(block.evmBlockNumber - this.lastOrderedBlock >= this.config.perf.maxBlocksBehind) {
            this.idleWorkers++;
            await sleep(200);
            this.idleWorkers--;
        }
    }

    /*
     * Entry point
     */
    async launch() {

        this.printIntroText();

        let startBlock = this.startBlock;
        let startEvmBlock = this.startBlock - this.config.evmDelta;
        let stopBlock = this.stopBlock;
        let prevHash;

        await this.connector.init();

        logger.info('checking db for blocks...');
        let lastBlock = await this.connector.getLastIndexedBlock();

        if (lastBlock != null) {  // if we find blocks on the db check for gaps...
            const gap = await this.connector.fullGapCheck();
            if (gap == null) {
                // no gaps found
                ({ startBlock, startEvmBlock, prevHash } = await this.getBlockInfoFromLastBlock(lastBlock));
            } else {
                ({ startBlock, startEvmBlock, prevHash } = await this.getBlockInfoFromGap(gap));
            }
        } else {
            prevHash = await this.getPreviousHash();
            logger.info(`start from ${startBlock} with hash 0x${prevHash}.`);
        }

        // Init state tracking attributes
        this.prevHash = prevHash;
        this.lastOrderedBlock = startEvmBlock - 1;
        this.lastNativeOrderedBlock = this.startBlock - 1;

        // Begin websocket consumtion
        this.reader.startProcessing({
            start_block_num: startBlock,
            end_block_num: stopBlock,
            max_messages_in_flight: this.config.perf.maxMsgsInFlight,
            irreversible_only: false,
            have_positions: [],
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true
        });

        // Launch bg routines
        setInterval(() => this.orderer(), 400);
        setInterval(() => this.updateDebugStats(), 1000);

    }

    /*
     * Poll remote rpc for genesis block, which is block previous to evm deployment
     */
    private async getGenesisBlock() {
        let genesisBlock = null;
        while(genesisBlock == null) {
            try {
                // get genesis information
                genesisBlock = await this.reader.rpc.get_block(
                    this.evmDeployBlock - 1);

            } catch (e) {
                logger.error(e);
                logger.warn(`couldn\'t get genesis block ${this.evmDeployBlock - 1} retrying in 5 sec...`);
                await sleep(5000);
                continue
            }
        }
        return genesisBlock;
    }

    /*
     * Get start parameters from last block indexed on db
     */
    private async getBlockInfoFromLastBlock(lastBlock: StorageEosioDelta): Promise<StartBlockInfo> {

        // found blocks on the database
        logger.info(`Last block found: ${JSON.stringify(lastBlock, null, 4)}`);

        let startBlock = lastBlock.block_num;
        let startEvmBlock = lastBlock['@global'].block_num;

        const startStr = formatBlockNumbers(startBlock, startEvmBlock);
        logger.info(`purge blocks newer than ${startStr}`);

        await this.connector.purgeNewerThan(startBlock, startEvmBlock);

        logger.info('done.');

        lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        if (lastBlock.block_num != (startBlock - 1))
            throw new Error(`Last block: ${lastBlock.blockNumsToString()}, is not ${startStr} - 1`);

        logger.info(
            `found! ${lastBlock.blockNumsToString()} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return { startBlock, startEvmBlock, prevHash };
    }

    /*
     * Get start parameters from first gap on database
     */
    private async getBlockInfoFromGap(gap: number): Promise<StartBlockInfo> {

        const firstBlock = await this.connector.getIndexedBlockEVM(gap);

        // found blocks on the database
        logger.info(`Last block of continuous range found: ${JSON.stringify(firstBlock, null, 4)}`);

        let startBlock = firstBlock.block_num;
        let startEvmBlock = firstBlock['@global'].block_num;

        const startStr = formatBlockNumbers(startBlock, startEvmBlock);
        logger.info(`purge blocks newer than ${startStr}`);

        await this.connector.purgeNewerThan(startBlock, startEvmBlock);

        logger.info('done.');

        const lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        if (lastBlock.block_num != (startBlock - 1))
            throw new Error(`Last block: ${lastBlock.blockNumsToString()}, is not ${startStr} - 1`);

        logger.info(
            `found! ${lastBlock.blockNumsToString()} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return { startBlock, startEvmBlock, prevHash };
    }

    /*
     * Get previous hash either from genesis or env/config
     */
    private async getPreviousHash(): Promise<string> {
        // prev blocks not found, start from genesis or EVM_PREV_HASH
        if (this.config.startBlock == this.config.evmDeployBlock) {
            let genesisBlock = await this.getGenesisBlock();

            logger.info('evm deployment native genesis block: ');
            logger.info(JSON.stringify(genesisBlock, null, 4));

            // number of seconds since epoch
            const genesisTimestamp = moment.utc(genesisBlock.timestamp).unix();

            const header = BlockHeader.fromHeaderData({
                'gasLimit': new BN(0),
                'number': new BN(this.evmDeployBlock - this.config.evmDelta - 1),
                'difficulty': new BN(0),
                'timestamp': new BN(genesisTimestamp),
                'extraData': Buffer.from(genesisBlock.id, 'hex'),
                'stateRoot': Buffer.from('56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421', 'hex')
            })

            this.ethGenesisHash = header.hash().toString('hex');

            logger.info('ethereum genesis block header: ');
            logger.info(JSON.stringify(header.toJSON(), null, 4));

            logger.info(`ethereum genesis hash: 0x${this.ethGenesisHash}`);
            return this.ethGenesisHash;
        } else if (this.config.evmPrevHash != '') {
            return this.config.evmPrevHash;
        } else {
            throw new Error('Configuration error, no way to get previous hash.  Must either start from genesis or provide a previous hash via config');
        }
    }

    /*
     * Detect forks and handle them, leave every state tracking attribute in a healthy state
     */
    private async maybeHandleFork(b: ProcessedBlock) {
        if (this.forked || b.nativeBlockNumber > this.lastNativeOrderedBlock)
            return;

        this.forked = true;

        logger.info('chain fork detected. reverse all blocks which were affected');

        // wait until all db connector write tasks finish
        while (this.connector.writeCounter > 0) {
            logger.debug(`waiting for ${this.connector.writeCounter} write operations to finish...`);
            await sleep(200);
        }

        // clear blocksQueue
        let iterB = this.maybeGetNewestBlock();
        while (iterB != null && iterB.nativeBlockNumber > b.nativeBlockNumber) {
            this.blocksQueue.dequeue();
            logger.debug(`deleted ${iterB.toString()} from blocksQueue`);
            iterB = this.maybeGetNewestBlock();
        }

        // finally purge db
        await this.connector.purgeNewerThan(b.nativeBlockNumber, b.evmBlockNumber);
        logger.debug(`purged db of blocks newer than ${b.toString()}, continue...`);

        const lastBlock = await this.connector.getLastIndexedBlock();

        if (lastBlock == null || lastBlock.block_num != (b.nativeBlockNumber - 1)) {
            throw new Error(
                `Error while handling fork, block number mismatch! last block: ${
                    JSON.stringify(lastBlock, null, 4)}`);
        }

        // tweak variables used by ordering machinery
        this.prevHash = lastBlock['@evmBlockHash'];
        this.lastOrderedBlock = lastBlock['@global'].block_num;
        this.lastNativeOrderedBlock = lastBlock.block_num;

        this.forked = false;
    }

    printIntroText() {
        logger.info('Telos EVM Indexer 1.5');
        logger.info(
            'Blocks will be shown in the following format: [native block num|evm block num]');
        logger.info('Happy indexing!');
    }
};
