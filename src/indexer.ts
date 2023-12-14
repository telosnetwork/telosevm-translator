import {readFileSync} from "node:fs";

import {HyperionSequentialReader} from "@eosrio/hyperion-sequential-reader";

import {IndexedBlockInfo, IndexerConfig, IndexerState, StartBlockInfo} from './types/indexer.js';

import {createLogger, format, Logger, transports} from 'winston';

import {StorageEosioAction} from './types/evm.js';

import {Connector} from './database/connector.js';

import {
    EMPTY_TRIE_BUF,
    generateBlockApplyInfo,
    hexStringToUint8Array,
    isTxDeserializationError,
    NULL_HASH,
    ProcessedBlock,
    StorageEosioDelta, TEVMBlockHeader,
} from './utils/evm.js'

import moment from 'moment';
import {JsonRpc, RpcInterfaces} from 'eosjs';
import {getRPCClient} from './utils/eosio.js';
import {ABI} from "@greymass/eosio";

import rlp from 'rlp';
import EventEmitter from "events";

import SegfaultHandler from 'segfault-handler';
import {sleep} from "./utils/indexer.js";

import workerpool from 'workerpool';
import {arrayToHex} from "eosjs/dist/eosjs-serialize.js";
import {keccak256} from "@ethereumjs/devp2p";


class TEVMEvents extends EventEmitter {}

export class TEVMIndexer {
    endpoint: string;  // nodeos http rpc endpoint
    wsEndpoint: string;  // nodoes ship ws endpoint

    evmBlockDelta: number;  // number diference between evm and native blck
    evmDeployBlock: number;  // native block number where telos.evm was deployed
    startBlock: number;  // native block number to start indexer from as defined by env vars or config
    stopBlock: number;  // native block number to stop indexer from as defined by env vars or config
    ethGenesisHash: string;  // calculated ethereum genesis hash

    genesisBlock: RpcInterfaces.GetBlockResult = null;

    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap

    config: IndexerConfig;  // global indexer config as defined by envoinrment or config file

    private reader: HyperionSequentialReader;  // websocket state history connector, deserializes nodeos protocol
    private rpc: JsonRpc;
    private remoteRpc: JsonRpc;
    connector: Connector;  // custom elastic search db driver

    private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    headBlock: number;
    lastBlock: number;  // last block number that was succesfully pushed to db in order

    // debug status used to print statistics
    private pushedLastUpdate: number = 0;
    private timestampLastUpdate: number;
    private stallCounter: number = 0;

    private statsTaskId: NodeJS.Timer;
    private stateSwitchTaskId: NodeJS.Timer;

    private irreversibleOnly: boolean;

    private logger: Logger;

    events = new TEVMEvents();

    private evmDeserializationPool;

    constructor(telosConfig: IndexerConfig) {
        this.config = telosConfig;

        this.endpoint = telosConfig.endpoint;
        this.wsEndpoint = telosConfig.wsEndpoint;

        this.evmBlockDelta = telosConfig.evmBlockDelta;
        this.evmDeployBlock = telosConfig.evmDeployBlock;

        this.startBlock = telosConfig.startBlock;
        this.stopBlock = telosConfig.stopBlock;
        this.rpc = getRPCClient(telosConfig.endpoint);
        this.remoteRpc = getRPCClient(telosConfig.remoteEndpoint);
        this.irreversibleOnly = telosConfig.irreversibleOnly || false;

        process.on('SIGINT', async () => await this.stop());
        process.on('SIGUSR1', () => this.resetReader());
        process.on('SIGQUIT', async () => await this.stop());
        process.on('SIGTERM', async () => await this.stop());

        SegfaultHandler.registerHandler(
            `translator-segfault-${process.pid}.log`);

        process.on('unhandledRejection', error => {
            // @ts-ignore
            if (error.message == 'Worker terminated')
                return;
            this.logger.error('Unhandled Rejection');
            try {
                this.logger.error(JSON.stringify(error, null, 4));
            } catch (e) {

            }
            // @ts-ignore
            this.logger.error(error.message);
            // @ts-ignore
            this.logger.error(error.stack);
            throw error;
        });

        // if (process.env.LOG_LEVEL == 'debug')
        //     process.on('SIGUSR1', async () => logWhyIsNodeRunning());

        this.timestampLastUpdate = Date.now() / 1000;
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    updateDebugStats() {
        const now = Date.now() / 1000;
        const timeElapsed = now - this.timestampLastUpdate;
        const blocksPerSecond = parseFloat((this.pushedLastUpdate / timeElapsed).toFixed(1));

        if (blocksPerSecond == 0)
            this.stallCounter++;

        if (this.stallCounter > this.config.perf.stallCounter) {
            this.logger.info('stall detected... restarting ship reader.');
            this.resetReader();
        }

        let statsString = `${this.lastBlock} pushed, at ${blocksPerSecond} blocks/sec`;
        const untilHead = this.headBlock - this.lastBlock;

        if (untilHead > 3) {
            const hoursETA = `${((untilHead / blocksPerSecond) / (60 * 60)).toFixed(1)}hs`;
            statsString += `, ${untilHead} to reach head, aprox ${hoursETA}`;
        }

        this.logger.info(statsString);

        this.pushedLastUpdate = 0;
        this.timestampLastUpdate = now;
    }

    resetReader() {
        this.logger.warn("restarting SHIP reader!...");
        this.reader.restart(0);
        this.stallCounter = -2;
    }

    /*
     * Generate valid ethereum has, requires blocks to be passed in order, updates state
     * handling class attributes.
     */
    async hashBlock(block: ProcessedBlock) {
        const evmTxs = block.evmTxs;

        // generate block info derived from applying the transactions to the vm state
        const blockApplyInfo = await generateBlockApplyInfo(evmTxs);
        const blockTimestamp = moment.utc(block.blockTimestamp);

        // generate 'valid' block header
        const blockHeader = TEVMBlockHeader.fromHeaderData({
            'parentHash': hexStringToUint8Array(this.prevHash),
            'transactionsTrie': blockApplyInfo.txsRootHash.root(),
            'receiptTrie': blockApplyInfo.receiptsTrie.root(),
            'stateRoot': EMPTY_TRIE_BUF,
            'logsBloom': blockApplyInfo.blockBloom.bitvector,
            'number': BigInt(block.evmBlockNumber),
            'gasLimit': blockApplyInfo.gasLimit,
            'gasUsed': blockApplyInfo.gasUsed,
            'difficulty': BigInt(0),
            'timestamp': BigInt(blockTimestamp.unix()),
            'extraData': hexStringToUint8Array(block.nativeBlockHash)
        })

        const currentBlockHash = arrayToHex(blockHeader.hash());
        const receiptsHash = arrayToHex(blockApplyInfo.receiptsTrie.root());
        const txsHash = arrayToHex(blockApplyInfo.txsRootHash.root());

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
                "@receiptsRootHash": receiptsHash,
                "@transactionsRoot": txsHash,
                "gasUsed": blockApplyInfo.gasUsed.toString(),
                "gasLimit": blockApplyInfo.gasLimit.toString(),
                "size": blockApplyInfo.size.toString()
            }),
            "nativeHash": block.nativeBlockHash.toLowerCase(),
            "parentHash": this.prevHash,
            "receiptsRoot": receiptsHash,
            "blockBloom": arrayToHex(blockApplyInfo.blockBloom.bitvector)
        };

        if (evmTxs.length > 0) {
            for (const evmTxData of evmTxs) {
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
        // SYNC & HEAD mode switch detection
        try {
            const remoteHead = (await this.remoteRpc.get_info()).head_block_num;
            const blocksUntilHead = remoteHead - this.lastBlock;

            this.logger.info(`${blocksUntilHead} until remote head ${remoteHead}`);

            if (blocksUntilHead <= 100) {
                if (this.state == IndexerState.HEAD)
                    return;

                this.state = IndexerState.HEAD;
                this.connector.state = IndexerState.HEAD;

                this.logger.info(
                    'switched to HEAD mode! blocks will be written to db asap.');

            } else {
                if (this.state == IndexerState.SYNC)
                    return;

                this.state = IndexerState.SYNC;
                this.connector.state = IndexerState.SYNC;

                this.logger.info(
                    'switched to SYNC mode! blocks will be written to db in batches.');
            }

        } catch (error) {
            this.logger.warn('get_info query to remote failed with error:');
            this.logger.warn(error);
        }
    }

    /*
     * HyperionSequentialReader emit block callback, gets blocks from ship in order.
     */
    async processBlock(block: any): Promise<void> {
        const currentBlock = block.blockInfo.this_block.block_num;

        if (currentBlock < this.startBlock) {
            this.reader.ack();
            return;
        }

        if (currentBlock > this.lastBlock + 1)
            throw new Error(
                `Expected block ${this.lastBlock + 1} and got ${currentBlock}, gap on reader?`)

        this.stallCounter = 0;

        // process deltas to catch evm block num
        const currentEvmBlock = currentBlock - this.config.evmBlockDelta;
        const evmTransactions = []
        const errors = []

        // traces
        const systemAccounts = ['eosio', 'eosio.stake', 'eosio.ram'];
        const contractWhitelist = [
            "eosio.evm", "eosio.token",  // evm
            "eosio.msig"  // deferred transaction sig catch
        ];
        const actionWhitelist = [
            "raw", "withdraw", "transfer",  // evm
            "exec" // msig deferred sig catch
        ]
        const actions = [];
        const txTasks = [];
        for (const action of block.actions) {
            const aDuplicate = actions.find(other => {
                return other.receipt.act_digest === action.receipt.act_digest
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

            if (action.act.account == "eosio.evm") {
                if (action.act.name == "raw") {
                    txTasks.push(
                        this.evmDeserializationPool.exec(
                            'createEvm', [
                                block.blockInfo.this_block.block_id,
                                evmTransactions.length,
                                currentEvmBlock,
                                action.act.data,
                                action.console,  // tx.trace.console
                            ]
                        )
                    );
                } else if (action.act.name == "withdraw") {
                    txTasks.push(
                        this.evmDeserializationPool.exec(
                            'createWithdraw', [
                                block.blockInfo.this_block.block_id,
                                evmTransactions.length,
                                currentEvmBlock,
                                action.act.data
                            ]
                        )
                    );
                }
            } else if (action.act.account == "eosio.token" &&
                action.act.name == "transfer" &&
                action.act.data.to == "eosio.evm") {
                txTasks.push(
                    this.evmDeserializationPool.exec(
                        'createDeposit', [
                            block.blockInfo.this_block.block_id,
                            evmTransactions.length,
                            currentEvmBlock,
                            action.act.data
                        ]
                    )
                );
            } else
                continue;

            actions.push(action);
        }

        const evmTxs = await Promise.all(txTasks);

        let gasUsedBlock = BigInt(0);
        let i = 0;
        for (const evmTx of evmTxs) {
            if (isTxDeserializationError(evmTx)) {
                this.logger.error(evmTx.info.error);
                throw new Error(JSON.stringify(evmTx));
            }

            gasUsedBlock += evmTx.gasused;
            evmTx.gasusedblock = gasUsedBlock.toString();

            const action = actions[i];
            evmTransactions.push({
                trx_id: action.trxId,
                action_ordinal: action.actionOrdinal,
                signatures: [],
                evmTx: evmTx
            });
            i++;
        }

        const newestBlock = new ProcessedBlock({
            nativeBlockHash: block.blockInfo.this_block.block_id,
            nativeBlockNumber: currentBlock,
            evmBlockNumber: currentEvmBlock,
            blockTimestamp: block.blockHeader.timestamp,
            evmTxs: evmTransactions,
            errors: errors
        });

        // fork handling
        if (currentBlock != this.lastBlock + 1)
            await this.handleFork(newestBlock);

        const storableBlockInfo = await this.hashBlock(newestBlock);

        // Update block num state tracking attributes
        this.lastBlock = currentBlock;

        // Push to db
        await this.connector.pushBlock(storableBlockInfo);
        this.events.emit('push-block', storableBlockInfo);

        // For debug stats
        this.pushedLastUpdate++;

        this.reader.ack();
    }

    async getOldHash(blockNum: number) {
        const block = await this.connector.getIndexedBlock(blockNum);
        if(!block)
            throw new Error(`Block #${blockNum} not found in db`);
        return block['@evmBlockHash'];
    }

    startReaderFrom(blockNum: number) {
        this.reader = new HyperionSequentialReader({
            poolSize: this.config.perf.readerWorkerAmount,
            shipApi: this.wsEndpoint,
            chainApi: this.config.endpoint,
            blockConcurrency: this.config.perf.readerWorkerAmount,
            blockHistorySize: this.config.blockHistorySize,
            startBlock: blockNum,
            irreversibleOnly: this.irreversibleOnly,
            logLevel: (this.config.readerLogLevel || 'info').toLowerCase()
        });

        this.reader.onConnected = () => {
            this.logger.info('SHIP Reader connected.');
        }
        this.reader.onDisconnect = () => {
            this.logger.warn('SHIP Reader disconnected.');
        }
        this.reader.onError = (err) => {
            this.logger.error(`SHIP Reader error: ${err}`);
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
        const options = {
            exitOnError: false,
            level: this.config.logLevel,
            format: format.combine(
                format.metadata(),
                format.colorize(),
                format.timestamp(),
                format.printf((info: any) => {
                    return `${info.timestamp} [PID:${process.pid}] [${info.level}] : ${info.message} ${Object.keys(info.metadata).length > 0 ? JSON.stringify(info.metadata) : ''}`;
                })
            )
        }
        this.logger = createLogger(options);
        this.logger.add(new transports.Console({
            level: this.config.logLevel
        }));
        this.logger.debug('Logger initialized with level ' + this.config.logLevel);

        this.printIntroText();

        let startBlock = this.startBlock;
        let prevHash;

        this.connector = new Connector(this.config, this.logger);
        await this.connector.init();

        if (process.argv.includes('--trim-from')) {
            const trimFromIndex = process.argv.indexOf('--trim-from');
            const trimBlockNum = parseInt(process.argv[trimFromIndex + 1], 10);
            await this.connector.purgeNewerThan(trimBlockNum);
        }

        this.logger.info('checking db for blocks...');
        let lastBlock = await this.connector.getLastIndexedBlock();

        let gap = null;
        if ((!process.argv.includes('--skip-integrity-check'))) {
            if (lastBlock != null) {
                this.logger.debug('performing integrity check...');
                gap = await this.connector.fullIntegrityCheck();

                if (gap == null) {
                    this.logger.info('NO GAPS FOUND');
                } else {
                    this.logger.info('GAP INFO:');
                    this.logger.info(JSON.stringify(gap, null, 4));
                }
            } else {
                if (process.argv.includes('--only-db-check')) {
                    this.logger.warn('--only-db-check on empty database...');
                    await this.connector.deinit();
                    return;
                }
            }
        }

        if (process.argv.includes('--only-db-check')) {
            this.logger.info('--only-db-check passed exiting...');
            await this.connector.deinit();
            return;
        }

        if (this.config.evmPrevHash === '') {
            if (lastBlock != null &&
                lastBlock['@evmPrevBlockHash'] != NULL_HASH) {

                if (gap == null) {
                    ({startBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
                } else {
                    if (process.argv.includes('--gaps-purge'))
                        ({startBlock, prevHash} = await this.getBlockInfoFromGap(gap));
                    else
                        throw new Error(
                            `Gap found in database at ${gap}, but --gaps-purge flag not passed!`);
                }

                // Init state tracking attributes
                this.prevHash = prevHash;
                this.startBlock = startBlock;
                this.lastBlock = startBlock - 1;
                this.connector.lastPushed = this.lastBlock;
            }

        } else {

            this.prevHash = this.config.evmPrevHash;
            this.startBlock = this.config.startBlock;
            this.lastBlock = startBlock - 1;
            this.connector.lastPushed = this.lastBlock;
        }

        if (prevHash)
            this.logger.info(`start from ${startBlock} with hash 0x${prevHash}.`);
        else {
            this.logger.info(`starting from genesis block ${startBlock}`);
            await this.genesisBlockInitialization();
        }

        // check node actually contains first block
        try {
            await this.rpc.get_block(startBlock);
        } catch (error) {
            if ((process.argv.length > 1) && (!process.argv.includes('--skip-start-block-check')))
                throw new Error(
                    `Error when doing start block check: ${error.message}`);
        }

        process.env.CHAIN_ID = this.config.chainId.toString();
        process.env.ENDPOINT = this.config.endpoint;
        process.env.LOG_LEVEL = this.config.logLevel;

        // this.evmDeserializationPool = new StaticPool({
        //     size: this.config.perf.evmWorkerAmount,
        //     task: './build/workers/evm.js',
        //     workerData: {chainId: this.config.chainId, logLevel: this.config.logLevel}
        // });

        this.evmDeserializationPool = workerpool.pool(
            './build/workers/handlers.js', {
            minWorkers: this.config.perf.evmWorkerAmount,
            maxWorkers: this.config.perf.evmWorkerAmount,
            workerType: 'thread'
        });

        this.logger.info('Initializing ws broadcast...')
        this.connector.startBroadcast();

        this.startReaderFrom(startBlock);

        // Launch bg routines
        this.statsTaskId = setInterval(() => this.updateDebugStats(), 1000);
        this.stateSwitchTaskId = setInterval(() => this.handleStateSwitch(), 10 * 1000);
    }

    async genesisBlockInitialization() {
        this.genesisBlock = await this.getGenesisBlock();

        // number of seconds since epoch
        const genesisTimestamp = moment.utc(this.genesisBlock.timestamp).unix();

        // genesis evm block num
        const genesisEvmBlockNum = this.genesisBlock.block_num - this.config.evmBlockDelta;

        const genesisParams = {
            "alloc": {},
            "config": {
                "chainID": this.config.chainId,
                "homesteadBlock": 0,
                "eip155Block": 0,
                "eip158Block": 0
            },
            "nonce": "0x0000000000000000",
            "difficulty": "0x00",
            "mixhash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "coinbase": "0x0000000000000000000000000000000000000000",
            "timestamp": "0x" + genesisTimestamp.toString(16),
            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "extraData": "0x" + this.genesisBlock.id,
            "gasLimit": "0xffffffff",
            "uncleHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "transactionsTrie": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "receiptTrie": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "logsBloom": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "number": "0x" + genesisEvmBlockNum.toString(16),
            "gasUsed": "0x00"
        }

        const encodedGenesisParams = rlp.encode([
            hexStringToUint8Array(genesisParams['parentHash']),
            hexStringToUint8Array(genesisParams['uncleHash']),
            hexStringToUint8Array(genesisParams['coinbase']),
            hexStringToUint8Array(genesisParams['stateRoot']),
            hexStringToUint8Array(genesisParams['transactionsTrie']),
            hexStringToUint8Array(genesisParams['receiptTrie']),
            hexStringToUint8Array(genesisParams['logsBloom']),
            hexStringToUint8Array(genesisParams['difficulty']),
            hexStringToUint8Array(genesisParams['number']),
            hexStringToUint8Array(genesisParams['gasLimit']),
            hexStringToUint8Array(genesisParams['gasUsed']),
            hexStringToUint8Array(genesisParams['timestamp']),
            hexStringToUint8Array(genesisParams['extraData']),
            hexStringToUint8Array(genesisParams['mixhash']),
            hexStringToUint8Array(genesisParams['nonce'])
        ]);
        const genesisHash = keccak256(encodedGenesisParams);

        this.ethGenesisHash = arrayToHex(genesisHash);

        if (this.config.evmValidateHash != "" &&
            this.ethGenesisHash != this.config.evmValidateHash) {
            throw new Error('FATAL!: Generated genesis hash doesn\'t match remote!');
        }

        // Init state tracking attributes
        this.prevHash = this.ethGenesisHash;
        this.lastBlock = this.genesisBlock.block_num;
        this.connector.lastPushed = this.lastBlock;

        this.logger.info('ethereum genesis params: ');
        this.logger.info(JSON.stringify(genesisParams, null, 4));

        this.logger.info(`ethereum genesis hash: 0x${this.ethGenesisHash}`);

        // if we are starting from genesis store block skeleton doc
        // for rpc to be able to find parent hash for fist block
        await this.connector.pushBlock({
            transactions: [],
            errors: [],
            delta: new StorageEosioDelta({
                '@timestamp': moment.utc(this.genesisBlock.timestamp).toISOString(),
                block_num: this.genesisBlock.block_num,
                '@global': {
                    block_num: genesisEvmBlockNum
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
            this.logger.debug(`waiting for ${this.connector.writeCounter} write operations to finish...`);
            await sleep(200);
        }
    }

    /*
     * Stop indexer gracefully
     */
    async stop() {
        clearInterval(this.statsTaskId as unknown as number);
        clearInterval(this.stateSwitchTaskId as unknown as number);

        if (this.reader) {
            try {
                this.reader.stop();
            } catch (e) {
                this.logger.warn(`error stopping reader: ${e.message}`);
            }
        }

        await this._waitWriteTasks();

        if (this.connector) {
            try {
                await this.connector.deinit();
            } catch (e) {
                this.logger.warn(`error stopping connector: ${e.message}`);
            }
        }

        if (this.evmDeserializationPool) {
            try {
                await this.evmDeserializationPool.terminate(true);
            } catch (e) {
                this.logger.warn(`error stopping thread pool: ${e.message}`);
            }
        }

        // if (process.env.LOG_LEVEL == 'debug')
        //     logWhyIsNodeRunning();
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
                    this.startBlock - 1);

            } catch (e) {
                this.logger.error(e);
                this.logger.warn(`couldn\'t get genesis block ${this.startBlock - 1} retrying in 5 sec...`);
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
        if (lastBlock.block_num != newlastBlock.block_num)
            throw new Error(
                'New last block check failed probably another indexer is running, abort...');

        let startBlock = lastBlock.block_num;

        this.logger.info(`purge blocks newer than ${startBlock}`);

        await this.connector._purgeBlocksNewerThan(startBlock);

        this.logger.info('done.');

        lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        this.logger.info(
            `found! ${lastBlock.block_num} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, prevHash};
    }

    /*
     * Get start parameters from first gap on database
     */
    private async getBlockInfoFromGap(gap: number): Promise<StartBlockInfo> {

        let firstBlock: StorageEosioDelta;
        let delta = 0;
        while (!firstBlock || firstBlock.block_num === undefined) {
            firstBlock = await this.connector.getIndexedBlock(gap - delta);
            delta++;
        }
        // found blocks on the database
        this.logger.info(`Last block of continuous range found: ${JSON.stringify(firstBlock, null, 4)}`);

        let startBlock = firstBlock.block_num;

        this.logger.info(`purge blocks newer than ${startBlock}`);

        await this.connector.purgeNewerThan(startBlock);

        this.logger.info('done.');

        const lastBlock = await this.connector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        if (lastBlock.block_num != (startBlock - 1))
            throw new Error(`Last block: ${lastBlock.block_num}, is not ${startBlock - 1} - 1`);

        this.logger.info(
            `found! ${lastBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, prevHash};
    }

    /*
     * Handle fork, leave every state tracking attribute in a healthy state
     */
    private async handleFork(b: ProcessedBlock) {
        const lastNonForked = b.nativeBlockNumber - 1;
        const forkedAt = this.lastBlock;

        this.logger.info(`got ${b.nativeBlockNumber} and expected ${this.lastBlock + 1}, chain fork detected. reverse all blocks which were affected`);

        await this._waitWriteTasks();

        // finally purge db
        await this.connector.purgeNewerThan(lastNonForked + 1);
        this.logger.debug(`purged db of blocks newer than ${lastNonForked}, continue...`);

        // tweak variables used by ordering machinery
        this.prevHash = await this.getOldHash(lastNonForked);
        this.lastBlock = lastNonForked;

        this.connector.forkCleanup(
            b.blockTimestamp,
            lastNonForked,
            forkedAt
        );
    }

    printIntroText() {
        this.logger.info('Telos EVM Translator v1.0.0-rc6');
        this.logger.info('Happy indexing!');
    }
};
