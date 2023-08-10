import {readFileSync} from "node:fs";


import {HyperionSequentialReader} from "@eosrio/hyperion-sequential-reader";

import {IndexedBlockInfo, IndexerConfig, IndexerState, StartBlockInfo} from './types/indexer.js';

import logger from './utils/winston.js';

import {StorageEosioAction, StorageEvmTransaction} from './types/evm.js';

import {Connector} from './database/connector.js';

import {
    BlockHeader,
    EMPTY_TRIE_BUF,
    EVMTxWrapper,
    formatBlockNumbers,
    generateBloom,
    generateReceiptRootHash,
    generateTxRootHash,
    getBlockGas,
    NULL_HASH,
    ProcessedBlock,
    StorageEosioDelta
} from './utils/evm.js'

import BN from 'bn.js';
import moment from 'moment';
import {JsonRpc, RpcInterfaces} from 'eosjs';
import {extractGlobalContractRow, getRPCClient} from './utils/eosio.js';
import {ABI} from "@greymass/eosio";


import {
    handleEvmDeposit,
    handleEvmTx,
    handleEvmWithdraw,
    isTxDeserializationError,
    setCommon,
    TxDeserializationError
} from "./handlers.js";


process.on('unhandledRejection', error => {
    logger.error('Unhandled Rejection');
    logger.error(JSON.stringify(error, null, 4));
    // @ts-ignore
    logger.error(error.message);
    // @ts-ignore
    logger.error(error.stack);
    process.exit(1);
});


const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));

interface InprogressBuffers {
    evmTransactions: Array<EVMTxWrapper>;
    errors: TxDeserializationError[];
    evmBlockNum: number;
};

export class TEVMIndexer {
    endpoint: string;  // nodeos http rpc endpoint
    wsEndpoint: string;  // nodoes ship ws endpoint

    evmDeployBlock: number;  // native block number where telos.evm was deployed
    startBlock: number;  // native block number to start indexer from as defined by env vars or config
    stopBlock: number;  // native block number to stop indexer from as defined by env vars or config
    ethGenesisHash: string;  // calculated ethereum genesis hash

    genesisBlock: RpcInterfaces.GetBlockResult = null;

    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap
    started: boolean = false;

    config: IndexerConfig;  // global indexer config as defined by envoinrment or config file

    private reader: HyperionSequentialReader;  // websocket state history connector, deserializes nodeos protocol
    private rpc: JsonRpc;
    private remoteRpc: JsonRpc;
    connector: Connector;  // custom elastic search db driver

    private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    headBlock: number;
    lastBlock: number;  // last evm block number that was succesfully pushed to db in order
    lastNativeBlock: number;  // last native block number that was succesfully pushed to db in order

    // debug status used to print statistics
    private pushedLastUpdate: number = 0;
    private timestampLastUpdate: number;
    private stallCounter: number = 0;

    private statsTaskId: NodeJS.Timer;

    private limboBuffs: InprogressBuffers = null;
    private irreversibleOnly: boolean;

    private latestBlockHashes: Array<{ blockNum: number, hash: string }> = [];

    constructor(telosConfig: IndexerConfig) {
        this.config = telosConfig;

        this.endpoint = telosConfig.endpoint;
        this.wsEndpoint = telosConfig.wsEndpoint;

        this.evmDeployBlock = telosConfig.evmDeployBlock;

        this.startBlock = telosConfig.startBlock;
        this.stopBlock = telosConfig.stopBlock;
        this.rpc = getRPCClient(telosConfig.endpoint);
        this.remoteRpc = getRPCClient(telosConfig.remoteEndpoint);
        this.connector = new Connector(telosConfig);
        this.irreversibleOnly = telosConfig.irreversibleOnly || false;

        process.on('SIGINT', async () => await this.stop());
        process.on('SIGUSR1', () => this.resetReader());
        process.on('SIGQUIT', async () => await this.stop());
        process.on('SIGTERM', async () => await this.stop());

        // if (process.env.LOG_LEVEL == 'debug')
        //     process.on('SIGUSR1', async () => logWhyIsNodeRunning());

        setCommon(telosConfig.chainId);

        this.timestampLastUpdate = Date.now() / 1000;
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    updateDebugStats() {
        const now = Date.now() / 1000;
        const timeElapsed = now - this.timestampLastUpdate;
        const blocksPerSecond = this.pushedLastUpdate / timeElapsed;

        if (blocksPerSecond == 0)
            this.stallCounter++;
        else
            this.stallCounter = 0;

        if (this.stallCounter > 10)
            this.resetReader();

        let statsString = `${formatBlockNumbers(this.lastNativeBlock, this.lastBlock)} pushed, at ${blocksPerSecond} blocks/sec`;
        const untilHead = this.headBlock - this.lastNativeBlock;

        if (untilHead > 3) {
            const hoursETA = `${((untilHead / blocksPerSecond) / (60 * 60)).toFixed(1)}hs`;
            statsString += `, ${untilHead} to reach head, aprox ${hoursETA}`;
        }

        logger.info(statsString);

        this.pushedLastUpdate = 0;
        this.timestampLastUpdate = now;
    }

    resetReader() {
        logger.warn("restarting SHIP reader!...");
        this.reader.stop();
        this.reader.mustReconnect = false;
        logger.warn("reader stopped, waiting 4 seconds to restart.");
        setTimeout(() => {
            this.startReaderFrom(this.lastNativeBlock + 1);
            this.reader.mustReconnect = true;
        }, 4000);
        this.stallCounter = -15;
    }

    /*
     * Generate valid ethereum has, requires blocks to be passed in order, updates state
     * handling class attributes.
     */
    async hashBlock(block: ProcessedBlock) {
        const evmTxs = block.evmTxs;

        // generate valid ethereum hashes
        const transactionsRoot = await generateTxRootHash(evmTxs);
        const receiptsRoot = await generateReceiptRootHash(evmTxs);
        const bloom = generateBloom(evmTxs);

        const {gasUsed, gasLimit, size} = getBlockGas(evmTxs);

        const blockTimestamp = moment.utc(block.blockTimestamp);

        // generate 'valid' block header
        const blockHeader = BlockHeader.fromHeaderData({
            'parentHash': Buffer.from(this.prevHash, 'hex'),
            'transactionsTrie': transactionsRoot,
            'receiptTrie': receiptsRoot,
            'stateRoot': EMPTY_TRIE_BUF,
            'bloom': bloom,
            'number': new BN(block.evmBlockNumber),
            'gasLimit': gasLimit,
            'gasUsed': gasUsed,
            'difficulty': new BN(0),
            'timestamp': new BN(blockTimestamp.unix()),
            'extraData': Buffer.from(block.nativeBlockHash, 'hex')
        })

        const currentBlockHash = blockHeader.hash().toString('hex');

        // debug stuff for hash match with 2.0
        //  const buffs = blockHeader.raw();
        //  let blockHeaderSize = 0;
        //  console.log(`raw buffs for block header with hash: \"${currentBlockHash}\"`);
        //  for (const [i, buf] of buffs.entries()) {
        //      console.log(`[${i}]: size ${buf.length}, \"${buf.toString('hex')}\"`);
        //      blockHeaderSize += buf.length;
        //  }
        //  console.log(`total header size: ${blockHeaderSize}`);


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
                "@evmPrevBlockHash": this.prevHash,
                "@evmBlockHash": currentBlockHash,
                "@blockHash": block.nativeBlockHash,
                "@receiptsRootHash": receiptsRoot.toString('hex'),
                "@transactionsRoot": transactionsRoot.toString('hex'),
                "gasUsed": gasUsed.toString(),
                "gasLimit": gasLimit.toString(),
                "size": size.toString()
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

    private async handleStateSwitch() {
        if (this.state == IndexerState.HEAD)
            return;

        // SYNC & HEAD mode swtich detection
        try {
            const remoteHead = (await this.remoteRpc.get_info()).head_block_num;
            const blocksUntilHead = remoteHead - this.lastBlock;

            logger.info(`${blocksUntilHead} until remote head ${remoteHead}`);

            if (blocksUntilHead <= 100) {
                this.state = IndexerState.HEAD;
                this.connector.state = IndexerState.HEAD;

                logger.info(
                    'switched to HEAD mode! blocks will be written to db asap.');
            }
        } catch (error) {
            logger.warn('get_info query to remote failed with error:');
            logger.warn(error);
        }
    }

    /*
     * State history on-block-deserialized call back, pushes blocks out of order
     * will sleep if block received is too far from last stored block.
     */
    async processBlock(block: any): Promise<void> {
        const currentBlock = block.blockInfo.this_block.block_num;

        if (currentBlock < this.startBlock) {
            this.reader.ack();
            return;
        }

        // process deltas to catch evm block num
        const globalDelta = extractGlobalContractRow(block.deltas)?.value;

        let buffs: InprogressBuffers = null;

        if (globalDelta) {
            const currentEvmBlock = globalDelta.block_num;

            // lazy initialization on genesis block case
            if (!this.started) {
                logger.info(`Got first evm block with num: ${currentEvmBlock}`);
                await this.genesisBlockInitialization(currentEvmBlock - 1);
                this.started = true;
            }

            buffs = {
                evmTransactions: [],
                errors: [],
                evmBlockNum: currentEvmBlock
            };

            if (this.limboBuffs != null) {
                for (const evmTx of this.limboBuffs.evmTransactions)
                    evmTx.evmTx.block = currentEvmBlock;

                buffs.evmTransactions = this.limboBuffs.evmTransactions
                buffs.errors = this.limboBuffs.errors;
                this.limboBuffs = null;
            }
        } else {
            if (!this.started) {
                this.reader.ack()
                logger.warn(`no global delta and !started skip block ${currentBlock}...`);
                return;
            }


            logger.warn(`onblock failed at block ${currentBlock}`);

            if (this.limboBuffs == null) {
                this.limboBuffs = {
                    evmTransactions: [],
                    errors: [],
                    evmBlockNum: 0
                };
            }

            buffs = this.limboBuffs;
        }

        if (!this.started)
            throw new Error(`Couldn't figure out genesis info before first block`);

        const evmBlockNum = buffs.evmBlockNum;
        const evmTransactions = buffs.evmTransactions;
        const errors = buffs.errors;

        // traces
        let gasUsedBlock = new BN(0);
        const systemAccounts = ['eosio', 'eosio.stake', 'eosio.ram'];
        const contractWhitelist = [
            "eosio.evm", "eosio.token",  // evm
            "eosio.msig"  // deferred transaction sig catch
        ];
        const actionWhitelist = [
            "raw", "withdraw", "transfer",  // evm
            "exec" // msig deferred sig catch
        ]
        const actDigests = [];
        for (const action of block.actions) {
            const aDuplicate = actDigests.find(digest => {
                return digest === action.receipt.act_digest
            })
            if (aDuplicate)
                continue;

            if (!contractWhitelist.includes(action.act.account) ||
                !actionWhitelist.includes(action.act.name))
                continue;

            // discard transfers to accounts other than eosio.evm
            // and transfers from system accounts
            if ((action.act.name == "transfer" && action.receiver != "eosio.evm") ||
                (action.act.name == "transfer" && action.act.data.from in systemAccounts))
                continue;


            let evmTx: StorageEvmTransaction | TxDeserializationError = null;
            if (action.act.account == "eosio.evm") {
                if (action.act.name == "raw") {
                    evmTx = await handleEvmTx(
                        block.blockInfo.this_block.block_id,
                        evmTransactions.length,
                        evmBlockNum,
                        action.act.data,
                        action.console,  // tx.trace.console,
                        gasUsedBlock
                    );
                } else if (action.act.name == "withdraw") {
                    evmTx = await handleEvmWithdraw(
                        block.blockInfo.this_block.block_id,
                        evmTransactions.length,
                        evmBlockNum,
                        action.act.data,
                        this.rpc,
                        gasUsedBlock
                    );
                }
            } else if (action.act.account == "eosio.token" &&
                action.act.name == "transfer" &&
                action.act.data.to == "eosio.evm") {
                evmTx = await handleEvmDeposit(
                    block.blockInfo.this_block.block_id,
                    evmTransactions.length,
                    evmBlockNum,
                    action.act.data,
                    this.rpc,
                    gasUsedBlock
                );
            } else
                continue;

            if (isTxDeserializationError(evmTx)) {
                logger.error(evmTx.info.error);
                throw new Error(JSON.stringify(evmTx));
            }

            gasUsedBlock.iadd(new BN(evmTx.gasused, 10));

            evmTransactions.push({
                trx_id: action.trxId,
                action_ordinal: action.actionOrdinal,
                signatures: [],
                evmTx: evmTx
            });
            actDigests.push(action.receipt.act_digest);
        }

        if (globalDelta == null) {
            this.reader.ack();
            return;
        }

        const newestBlock = new ProcessedBlock({
            nativeBlockHash: block.blockInfo.this_block.block_id,
            nativeBlockNumber: currentBlock,
            evmBlockNumber: evmBlockNum,
            blockTimestamp: block.blockHeader.timestamp,
            evmTxs: evmTransactions,
            errors: errors
        });

        await this.maybeHandleFork(newestBlock);
        const storableBlockInfo = await this.hashBlock(newestBlock);

        this.latestBlockHashes.push(
            {blockNum: currentBlock, hash: storableBlockInfo.delta["@evmBlockHash"]}
        );
        if (this.latestBlockHashes.length > 1000)
            this.latestBlockHashes.shift()

        // Push to db
        await this.connector.pushBlock(storableBlockInfo);

        // Update block num state tracking attributes
        this.lastBlock = evmBlockNum;
        this.lastNativeBlock = storableBlockInfo.delta.block_num;

        // For debug stats
        this.pushedLastUpdate++;

        this.reader.ack();
    }

    getOldHash(blockNum: number) {
        for (const iterBlock of this.latestBlockHashes) {
            if (iterBlock.blockNum == blockNum)
                return iterBlock.hash;
        }
        throw new Error('hash not found on cache!');
    }

    startReaderFrom(blockNum: number) {
        this.reader = new HyperionSequentialReader({
            poolSize: this.config.perf.workerAmount,
            shipApi: this.wsEndpoint,
            chainApi: this.config.endpoint,
            blockConcurrency: this.config.perf.workerAmount,
            startBlock: blockNum,
            irreversibleOnly: this.irreversibleOnly
        });

        this.reader.onConnected = () => {
            logger.info('SHIP Reader connected.');
        }
        this.reader.onDisconnect = () => {
            logger.warn('SHIP Reader disconnected.');
            logger.warn(`Retrying in 5 seconds... attempt number ${this.reader.reconnectCount}.`)
        }
        this.reader.onError = (err) => {
            logger.error(`SHIP Reader error: ${err}`);
        }

        this.reader.events.on('block', this.processBlock.bind(this));

        ['eosio', 'eosio.token', 'eosio.msig', 'eosio.evm'].forEach(c => {
            const abi = ABI.from(JSON.parse(readFileSync(`src/abis/${c}.json`).toString()));
            this.reader.addContract(c, abi);
        })
        this.reader.start();
    }

    /*
     * Entry point
     */
    async launch() {

        this.printIntroText();

        let startBlock = this.startBlock;
        let prevHash, startEvmBlock;

        await this.connector.init();

        logger.info('checking db for blocks...');
        let lastBlock = await this.connector.getLastIndexedBlock();
        logger.debug(`lastBlock: \n${JSON.stringify(lastBlock, null, 4)}`);

        if (lastBlock != null &&
            lastBlock['@evmPrevBlockHash'] != NULL_HASH) {

            if ((process.argv.length > 1) && (!process.argv.includes('--skip-integrity-check'))) {
                // if we find blocks on the db check,
                // integrity and return gap if present...
                logger.debug('performing integrity check...');
                const gap = await this.connector.fullIntegrityCheck();
                if (gap == null) {
                    // no gaps found
                    logger.info('no gaps found.');
                    ({startBlock, startEvmBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
                } else {
                    if ((process.argv.length > 1) && (process.argv.includes('--gaps-purge')))
                        ({startBlock, startEvmBlock, prevHash} = await this.getBlockInfoFromGap(gap));
                    else {
                        logger.warn(`Gap found in database at ${gap}, but --gaps-purge flag not passed!`);
                        process.exit(1);
                    }
                }
            } else {
                ({startBlock, startEvmBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
            }

            // Init state tracking attributes
            this.prevHash = prevHash;
            this.startBlock = startBlock;
            this.lastBlock = startEvmBlock - 1;
            this.lastNativeBlock = startBlock - 1;
            this.connector.lastPushed = startEvmBlock - 1;

            this.started = true
        }

        // check node actually contains first block
        try {
            await this.rpc.get_block(startBlock);
        } catch (error) {
            if ((process.argv.length > 1) && (!process.argv.includes('--skip-start-block-check')))
                throw new Error(
                    `Error when doing start block check: ${error.message}`);
        }

        setInterval(() => this.handleStateSwitch(), 10 * 1000);

        await this.startReaderFrom(startBlock);

        // Launch bg routines
        this.statsTaskId = setInterval(() => this.updateDebugStats(), 1000);
    }

    async genesisBlockInitialization(evmGenesisBlock: number) {
        this.genesisBlock = await this.getGenesisBlock();

        // number of seconds since epoch
        const genesisTimestamp = moment.utc(this.genesisBlock.timestamp).unix();

        const header = BlockHeader.fromHeaderData({
            'gasLimit': new BN(0),
            'number': new BN(evmGenesisBlock),
            'difficulty': new BN(0),
            'timestamp': new BN(genesisTimestamp),
            'extraData': Buffer.from(this.genesisBlock.id, 'hex'),
            'stateRoot': EMPTY_TRIE_BUF,
            'transactionsTrie': EMPTY_TRIE_BUF,
            'receiptTrie': EMPTY_TRIE_BUF
        })

        this.ethGenesisHash = header.hash().toString('hex');

        // Init state tracking attributes
        this.prevHash = this.ethGenesisHash;
        this.lastBlock = evmGenesisBlock;
        this.lastNativeBlock = this.startBlock - 1;
        this.connector.lastPushed = evmGenesisBlock;

        logger.info('ethereum genesis block header: ');
        logger.info(JSON.stringify(header.toJSON(), null, 4));

        logger.info(`ethereum genesis hash: 0x${this.ethGenesisHash}`);

        // if we are starting from genesis store block skeleton doc
        // for rpc to be able to find parent hash for fist block
        await this.connector.pushBlock({
            transactions: [],
            errors: [],
            delta: new StorageEosioDelta({
                '@timestamp': moment.utc(this.genesisBlock.timestamp).toISOString(),
                block_num: this.genesisBlock.block_num,
                '@global': {
                    block_num: evmGenesisBlock
                },
                '@blockHash': this.genesisBlock.id.toLowerCase(),
                '@evmPrevBlockHash': NULL_HASH,
                '@evmBlockHash': this.ethGenesisHash,
            }),
            nativeHash: this.genesisBlock.id.toLowerCase(),
            parentHash: '',
            receiptsRoot: '',
            blockBloom: ''
        })
    }

    /*
     * Wait until all db connector write tasks finish
     */
    async _waitWriteTasks() {
        while (this.connector.writeCounter > 0) {
            logger.debug(`waiting for ${this.connector.writeCounter} write operations to finish...`);
            await sleep(200);
        }
    }

    /*
     * Stop indexer gracefully
     */
    async stop() {
        // if (process.env.LOG_LEVEL == 'debug')
        //     logWhyIsNodeRunning();

        clearInterval(this.statsTaskId);

        await this._waitWriteTasks();

        process.exit(0);
    }

    /*
     * Poll remote rpc for genesis block, which is block previous to evm deployment
     */
    private async getGenesisBlock() {
        let genesisBlock = null;
        while (genesisBlock == null) {
            try {
                // get genesis information
                genesisBlock = await this.rpc.get_block(
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

        // sleep, then get last block again, if block_num changes it means
        // another indexer is running
        await sleep(3000);
        const newlastBlock = await this.connector.getLastIndexedBlock();
        if (lastBlock.block_num != newlastBlock.block_num) {
            logger.error(
                'New last block check failed probably another indexer is running, abort...');

            process.exit(2);
        }

        let startBlock = lastBlock.block_num;
        let startEvmBlock = lastBlock['@global'].block_num;

        const startStr = formatBlockNumbers(startBlock, startEvmBlock);
        logger.info(`purge blocks newer than ${startStr}`);

        await this.connector._purgeBlocksNewerThan(startBlock, startEvmBlock);

        logger.info('done.');

        lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        logger.info(
            `found! ${lastBlock.blockNumsToString()} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, startEvmBlock, prevHash};
    }

    /*
     * Get start parameters from first gap on database
     */
    private async getBlockInfoFromGap(gap: number): Promise<StartBlockInfo> {

        let firstBlock: StorageEosioDelta;
        let delta = 0;
        while (!firstBlock || firstBlock.block_num === undefined) {
            firstBlock = await this.connector.getIndexedBlockEVM(gap - delta);
            delta++;
        }
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

        return {startBlock, startEvmBlock, prevHash};
    }

    /*
     * Detect forks and handle them, leave every state tracking attribute in a healthy state
     */
    private async maybeHandleFork(b: ProcessedBlock) {
        if (b.nativeBlockNumber > this.lastNativeBlock ||
            b.nativeBlockNumber == this.startBlock)
            return;

        const lastNonForkedEvm = b.evmBlockNumber - 1;
        const lastNonForked = b.nativeBlockNumber - 1;
        const forkedAt = this.lastNativeBlock;

        logger.info(`got ${b.nativeBlockNumber} and expected ${this.lastNativeBlock}, chain fork detected. reverse all blocks which were affected`);

        await this._waitWriteTasks();

        // finally purge db
        await this.connector.purgeNewerThan(
            lastNonForked + 1,
            lastNonForkedEvm + 1
        );
        logger.debug(`purged db of blocks newer than ${lastNonForked}, continue...`);

        // tweak variables used by ordering machinery
        this.prevHash = this.getOldHash(lastNonForked);
        this.lastBlock = lastNonForkedEvm;
        this.lastNativeBlock = lastNonForked;

        this.connector.forkCleanup(
            b.blockTimestamp,
            b.evmBlockNumber,
            b.nativeBlockNumber,
            forkedAt
        );
    }

    printIntroText() {
        logger.info('Telos EVM Indexer 1.5');
        logger.info(
            'Blocks will be shown in the following format: [native block num|evm block num]');
        logger.info('Happy indexing!');
    }
};
