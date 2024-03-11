import {readFileSync} from "node:fs";

import {HyperionSequentialReader, ThroughputMeasurer} from "@telosnetwork/hyperion-sequential-reader";
import {createLogger, format, Logger, transports} from 'winston';
import {TEVMBlockHeader} from "telos-evm-custom-ds";
import {JsonRpc, RpcInterfaces} from 'eosjs';
import * as evm from "@ethereumjs/common";
import cloneDeep from "lodash.clonedeep";
import {clearInterval} from "timers";
import {Bloom} from "@ethereumjs/vm";
import {ABI} from "@greymass/eosio";
import workerpool from 'workerpool';
import moment from 'moment';

import {DEFAULT_CONF, IndexedBlockInfo, IndexerConfig, IndexerState, StartBlockInfo} from './types/indexer.js';
import {StorageEosioAction, StorageEosioActionSchema, StorageEosioDelta} from './types/evm.js';
import {BlockData, Connector} from './database/connector.js';
import {
    arrayToHex,
    generateBlockApplyInfo,
    hexStringToUint8Array,
    isTxDeserializationError,
    ZERO_HASH,
    ProcessedBlock, removeHexPrefix, BLOCK_GAS_LIMIT, EMPTY_TRIE, EMPTY_TRIE_BUF
} from './utils/evm.js'

import EventEmitter from "events";

import {packageInfo, sleep} from "./utils/indexer.js";
import {HandlerArguments} from "./workers/handlers.js";
import {getRPCClient} from './utils/eosio.js';
import {mergeDeep} from "./utils/misc.js";
import {expect} from "chai";

EventEmitter.defaultMaxListeners = 1000;

export class TEVMIndexer {
    endpoint: string;  // nodeos http rpc endpoint
    wsEndpoint: string;  // nodoes ship ws endpoint

    evmBlockDelta: number;  // number diference between evm and native blck
    startBlock: number;  // native block number to start indexer from as defined by env vars or config
    stopBlock: number;  // native block number to stop indexer from as defined by env vars or config
    ethGenesisHash: string;  // calculated ethereum genesis hash

    genesisBlock: RpcInterfaces.GetBlockResult = null;

    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap

    config: IndexerConfig;  // global indexer config as defined by envoinrment or config file

    private reader: HyperionSequentialReader;  // websocket state history connector, deserializes nodeos protocol
    private readerAbis: {account: string, abi: ABI}[];

    private rpc: JsonRpc;
    private remoteRpc: JsonRpc;
    connector: Connector;  // custom elastic search db driver
    reindexConnector: Connector = undefined;

    private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    headBlock: number;
    lastBlock: number;  // last block number that was succesfully pushed to db in order

    // debug status used to print statistics
    private pushedLastUpdate: number = 0;
    private stallCounter: number = 0;
    private perfMetrics: ThroughputMeasurer = new ThroughputMeasurer({
        windowSizeMs: 10 * 1000
    });

    private perfTaskId: NodeJS.Timer;
    private stateSwitchTaskId: NodeJS.Timer;
    private reindexPerfTaskId: NodeJS.Timeout;

    private readonly irreversibleOnly: boolean;

    private logger: Logger;

    events = new EventEmitter();

    private evmDeserializationPool;

    private readonly common: evm.Common;
    private _isRestarting: boolean = false;

    constructor(telosConfig: IndexerConfig) {
        this.config = telosConfig;
        this.common = evm.Common.custom({
            chainId: telosConfig.chainId,
            defaultHardfork: evm.Hardfork.Istanbul
        }, {baseChain: evm.Chain.Mainnet});

        this.endpoint = telosConfig.endpoint;
        this.wsEndpoint = telosConfig.wsEndpoint;

        this.evmBlockDelta = telosConfig.evmBlockDelta;

        this.startBlock = telosConfig.startBlock;
        this.stopBlock = telosConfig.stopBlock;
        this.rpc = getRPCClient(telosConfig.endpoint);
        this.remoteRpc = getRPCClient(telosConfig.remoteEndpoint);
        this.irreversibleOnly = telosConfig.irreversibleOnly || false;

        this.readerAbis = ['eosio', 'eosio.token', 'eosio.msig', 'telos.evm'].map(abiName => {
            const jsonAbi = JSON.parse(readFileSync(`src/abis/${abiName}.json`).toString());
            return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
        });

        process.on('SIGINT', async () => await this.stop());
        process.on('SIGUSR1', async () => await this.resetReader());
        process.on('SIGQUIT', async () => await this.stop());
        process.on('SIGTERM', async () => await this.stop());

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

        const loggingOptions = {
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
        this.logger = createLogger(loggingOptions);
        this.logger.add(new transports.Console({
            level: this.config.logLevel
        }));
        this.logger.debug('Logger initialized with level ' + this.config.logLevel);
        this.connector = new Connector(this.config, this.logger);
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    async performanceMetricsTask() {

        if (this.perfMetrics.max > 0) {
            if (this.perfMetrics.average == 0)
                this.stallCounter++;

            if (this.stallCounter > this.config.perf.stallCounter) {
                this.logger.info('stall detected... restarting ship reader.');
                await this.resetReader();
            }
        }

        const avgSpeed = this.perfMetrics.average.toFixed(2);
        let statsString = `${this.lastBlock.toLocaleString()} pushed, at ${avgSpeed} blocks/sec avg`;

        this.logger.info(statsString);

        this.perfMetrics.measure(this.pushedLastUpdate);
        this.pushedLastUpdate = 0;
    }

    async resetReader() {
        this.logger.warn("restarting SHIP reader!...");
        this._isRestarting = true;
        this.stallCounter = -2;
        this.reader.restart(1000, this.lastBlock + 1);
        await new Promise<void>((resolve, reject) => {
            this.reader.events.once('restarted', () => {
                resolve();
            });
            this.reader.events.once('error', (error) => {
                reject(error);
            });
        });
        this._isRestarting = false;
    }

    /*
     * Generate valid ethereum has, requires blocks to be passed in order, updates state
     * handling class attributes.
     */
    async hashBlock(block: ProcessedBlock): Promise<IndexedBlockInfo> {
        const evmTxs = block.evmTxs;

        // generate block info derived from applying the transactions to the vm state
        const blockApplyInfo = await generateBlockApplyInfo(evmTxs);
        const blockTimestamp = moment.utc(block.blockTimestamp);

        // generate 'valid' block header
        const blockHeader = TEVMBlockHeader.fromHeaderData({
            'parentHash': hexStringToUint8Array(this.prevHash),
            'transactionsTrie': blockApplyInfo.txsRootHash.root(),
            'receiptTrie': blockApplyInfo.receiptsTrie.root(),
            'logsBloom': blockApplyInfo.blockBloom.bitvector,
            'number': BigInt(block.evmBlockNumber),
            'gasLimit': BLOCK_GAS_LIMIT,
            'gasUsed': blockApplyInfo.gasUsed,
            'timestamp': BigInt(blockTimestamp.unix()),
            'extraData': hexStringToUint8Array(block.nativeBlockHash)
        }, {common: this.common});

        const currentBlockHash = arrayToHex(blockHeader.hash());
        const receiptsHash = arrayToHex(blockApplyInfo.receiptsTrie.root());
        const txsHash = arrayToHex(blockApplyInfo.txsRootHash.root());

        // generate storeable block info
        const storableActions: StorageEosioAction[] = [];
        const storableBlockInfo: IndexedBlockInfo = {
            "transactions": storableActions,
            "errors": block.errors,
            "delta": {
                "@timestamp": blockTimestamp.format(),
                "block_num": block.nativeBlockNumber,
                "@global": {
                    "block_num": block.evmBlockNumber
                },
                "@evmPrevBlockHash": this.prevHash,
                "@evmBlockHash": currentBlockHash,
                "@blockHash": block.nativeBlockHash,
                "@receiptsRootHash": receiptsHash,
                "@transactionsRoot": txsHash,
                "gasUsed": blockApplyInfo.gasUsed.toString(),
                "gasLimit": BLOCK_GAS_LIMIT.toString(),
                "size": blockApplyInfo.size.toString(),
                "txAmount": storableActions.length
            },
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
            this.headBlock = (await this.remoteRpc.get_info()).head_block_num;
            const isHeadTarget = this.headBlock >= this.stopBlock;
            const targetBlock = isHeadTarget ? this.headBlock : this.stopBlock;

            const blocksUntilHead = targetBlock - this.lastBlock;

            let statsString = `${blocksUntilHead.toLocaleString()} until target block ${targetBlock.toLocaleString()}`;
            if (this.perfMetrics.max != 0)
                statsString += ` ETA: ${moment.duration(blocksUntilHead / this.perfMetrics.average, 'seconds').humanize()}`;

            this.logger.info(statsString);

            if (isHeadTarget && blocksUntilHead <= 100) {
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

        if (this._isRestarting) {
            this.logger.warn(`dropped ${currentBlock} due to restart...`);
            return;
        }

        if (currentBlock < this.startBlock) {
            this.reader.ack();
            return;
        }

        if (this.stopBlock > 0 && currentBlock > this.stopBlock)
            return;

        if (currentBlock > this.lastBlock + 1) {
            this.logger.warn(`Expected block ${this.lastBlock + 1} and got ${currentBlock}, gap on reader?`);
            await this.resetReader();
            return;
        }

        this.stallCounter = 0;

        // native-evm block num delta is constant based on config
        const currentEvmBlock = currentBlock - this.config.evmBlockDelta;
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
        const startTxTask = (taskType: string, params: HandlerArguments) => {
            txTasks.push(
                this.evmDeserializationPool.exec(taskType, [params])
                    .catch((err) => {
                        this.logger.error(err.message);
                        this.logger.error(err.stack);
                        throw err;
                    })
            );
        };
        for (const action of block.actions) {
            const aDuplicate = actions.find(other => {
                return other.receipt.act_digest === action.receipt.act_digest
            })
            if (aDuplicate)
                continue;

            if (!contractWhitelist.includes(action.act.account) ||
                !actionWhitelist.includes(action.act.name))
                continue;

            const isEvmContract = action.act.account === 'eosio.evm';
            const isRaw = isEvmContract && action.act.name === 'raw';
            const isWithdraw = isEvmContract && action.act.name === 'withdraw';

            const isTokenContract = action.act.account === 'eosio.token';
            const isTransfer = isTokenContract && action.act.name === 'transfer';
            const isDeposit = isTransfer && action.act.data.to === 'eosio.evm';

            // discard transfers to accounts other than eosio.evm
            // and transfers from system accounts
            if ((isTransfer && action.receiver != 'eosio.evm') ||
                (isTransfer && action.act.data.from in systemAccounts))
                continue;

            const params: HandlerArguments = {
                nativeBlockHash: block.blockInfo.this_block.block_id,
                trx_index: txTasks.length,
                blockNum: currentEvmBlock,
                tx: action.act.data,
                consoleLog: action.console
            };

            if (isRaw)
                startTxTask('createEvm', params);

            else if (isWithdraw)
                startTxTask('createWithdraw', params);

            else if (isDeposit)
                startTxTask('createDeposit', params);

            else
                continue;

            actions.push(action);
        }

        const evmTxs = await Promise.all(txTasks);
        const evmTransactions = []

        let gasUsedBlock = BigInt(0);
        let i = 0;
        for (const evmTx of evmTxs) {
            if (isTxDeserializationError(evmTx)) {
                this.logger.error('ds workerpool error:')
                this.logger.error(evmTx.message);
                this.logger.error(evmTx.stack);
                this.logger.error('evmTx ds error info');
                const errInfo = evmTx.info;
                this.logger.error(`block_num: ${errInfo.block_num}`);
                throw evmTx;
            }

            gasUsedBlock += BigInt(evmTx.gasused);
            evmTx.gasusedblock = gasUsedBlock.toString();

            const action = actions[i];
            evmTransactions.push({
                action_ordinal: action.actionOrdinal,
                trx_id: action.trxId,
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

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // fork handling
        if (currentBlock < this.lastBlock + 1)
            await this.handleFork(newestBlock);

        const storableBlockInfo = await this.hashBlock(newestBlock);

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // Update block num state tracking attributes
        this.lastBlock = currentBlock;

        // Push to db
        await this.connector.pushBlock(storableBlockInfo);
        this.events.emit('push-block', storableBlockInfo);

        if (currentBlock == this.stopBlock) {
            await this.stop();
            this.events.emit('stop');
            return;
        }

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

    async startReaderFrom(blockNum: number) {
        this.reader = new HyperionSequentialReader({
            shipApi: this.wsEndpoint,
            chainApi: this.config.endpoint,
            poolSize: this.config.perf.readerWorkerAmount,
            blockConcurrency: this.config.perf.readerWorkerAmount,
            blockHistorySize: this.config.blockHistorySize,
            startBlock: blockNum,
            endBlock: this.config.stopBlock,
            actionWhitelist: {
                'eosio.token': ['transfer'],
                'eosio.msig': ['exec'],
                'eosio.evm': ['raw', 'withdraw']
            },
            tableWhitelist: {},
            irreversibleOnly: this.irreversibleOnly,
            logLevel: (this.config.readerLogLevel || 'info').toLowerCase(),
            maxPayloadMb: Math.floor(1024 * 1.5),
            skipInitialBlockCheck: true
        });

        this.reader.addContracts(this.readerAbis);

        this.reader.onConnected = () => {
            this.logger.info('SHIP Reader connected.');
        }
        this.reader.onDisconnect = () => {
            this.logger.warn('SHIP Reader disconnected.');
        }
        this.reader.onError = (err) => {
            this.logger.error(`SHIP Reader error: ${err}`);
            this.logger.error(err.stack);
        }

        this.reader.events.on('block', this.processBlock.bind(this));
        await this.reader.start();
    }

    /*
     * Entry point
     */
    async launch() {
        this.printIntroText();

        let startBlock = this.startBlock;
        let prevHash: string;

        await this.connector.init();

        if (this.config.runtime.trimFrom) {
            const trimBlockNum = this.config.runtime.trimFrom;
            await this.connector.purgeNewerThan(trimBlockNum);
        }

        this.logger.info('checking db for blocks...');
        let lastBlock = await this.connector.getLastIndexedBlock();

        let gap = null;
        if (!this.config.runtime.skipIntegrityCheck) {
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
                if (this.config.runtime.onlyDBCheck) {
                    this.logger.warn('--only-db-check on empty database...');
                }
            }
        }

        if (this.config.runtime.onlyDBCheck) {
            this.logger.info('--only-db-check passed exiting...');
            await this.connector.deinit();
            return;
        }

        if (this.config.runtime.reindex) {
            await this.reindex(
                this.config.runtime.reindex.into,
                {
                    eval: this.config.runtime.reindex.eval,
                    trimFrom: this.config.runtime.reindex.trimFrom
                }
            );
            await this.connector.deinit();
            return;
        }

        if (lastBlock != null &&
            lastBlock['@evmPrevBlockHash'] != ZERO_HASH) {
            // if there is a last block found on db other than genesis doc

            if (gap == null) {
                ({startBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
            } else {
                if (this.config.runtime.gapsPurge)
                    ({startBlock, prevHash} = await this.getBlockInfoFromGap(gap));
                else
                    throw new Error(
                        `Gap found in database at ${gap}, but --gaps-purge flag not passed!`);
            }

            this.prevHash = prevHash;
            this.startBlock = startBlock;
            this.lastBlock = startBlock - 1;
            this.connector.lastPushed = this.lastBlock;

        } else if (this.config.evmPrevHash != '') {
            // if there is an evmPrevHash set state directly
            prevHash = this.config.evmPrevHash;
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

        if (!this.config.runtime.skipStartBlockCheck) {
            // check node actually contains first block
            try {
                await this.rpc.get_block(startBlock);
            } catch (error) {
                throw new Error(
                    `Error when doing start block check: ${error.message}`);
            }
        }

        if (!this.config.runtime.skipRemoteCheck) {
            // check remote node is up
            try {
                await this.remoteRpc.get_info();
            } catch (error) {
                this.logger.error(`Error while doing remote node check: ${error.message}`);
                throw error;
            }
        }

        process.env.CHAIN_ID = this.config.chainId.toString();
        process.env.ENDPOINT = this.config.endpoint;
        process.env.LOG_LEVEL = this.config.logLevel;

        this.evmDeserializationPool = workerpool.pool(
            './build/workers/handlers.js', {
            minWorkers: this.config.perf.evmWorkerAmount,
            maxWorkers: this.config.perf.evmWorkerAmount,
            workerType: 'thread'
        });

        this.logger.info('Initializing ws broadcast...')
        this.connector.startBroadcast();

        await this.startReaderFrom(startBlock);

        // Launch bg routines
        this.perfTaskId = setInterval(async () => await this.performanceMetricsTask(), 1000);
        this.stateSwitchTaskId = setInterval(() => this.handleStateSwitch(), 10 * 1000);
    }

    async genesisBlockInitialization() {
        this.genesisBlock = await this.getGenesisBlock();

        // number of seconds since epoch
        const genesisTimestamp = moment.utc(this.genesisBlock.timestamp).unix();

        // genesis evm block num
        const genesisEvmBlockNum = this.genesisBlock.block_num - this.config.evmBlockDelta;

        const genesisHeader = TEVMBlockHeader.fromHeaderData({
            'number': BigInt(genesisEvmBlockNum),
            'gasLimit': BLOCK_GAS_LIMIT,
            'timestamp': BigInt(genesisTimestamp),
            'extraData': hexStringToUint8Array(this.genesisBlock.id)
        }, {common: this.common});

        const genesisHash = genesisHeader.hash();

        this.ethGenesisHash = arrayToHex(genesisHash);

        if (this.config.evmValidateHash != "" &&
            this.ethGenesisHash != this.config.evmValidateHash) {
            throw new Error('FATAL!: Generated genesis hash doesn\'t match remote!');
        }

        // Init state tracking attributes
        this.prevHash = this.ethGenesisHash;
        this.lastBlock = this.genesisBlock.block_num;
        this.connector.lastPushed = this.lastBlock;

        this.logger.info('ethereum genesis header: ');
        this.logger.info(JSON.stringify(genesisHeader.toJSON(), null, 4));

        this.logger.info(`ethereum genesis hash: 0x${this.ethGenesisHash}`);

        // if we are starting from genesis store block skeleton doc
        // for rpc to be able to find parent hash for fist block
        await this.connector.pushBlock({
            transactions: [],
            errors: [],
            delta: {
                '@timestamp': moment.utc(this.genesisBlock.timestamp).toISOString(),
                block_num: this.genesisBlock.block_num,
                '@global': {
                    block_num: genesisEvmBlockNum
                },
                '@blockHash': this.genesisBlock.id.toLowerCase(),
                '@evmPrevBlockHash': removeHexPrefix(ZERO_HASH),
                '@evmBlockHash': this.ethGenesisHash,
                "@receiptsRootHash": EMPTY_TRIE,
                "@transactionsRoot": EMPTY_TRIE,
                "gasUsed": "0",
                "gasLimit": BLOCK_GAS_LIMIT.toString(),
                "size": "0"
            },
            nativeHash: this.genesisBlock.id.toLowerCase(),
            parentHash: '',
            receiptsRoot: '',
            blockBloom: ''
        });

        this.events.emit('start');
    }

    private reindexBlock(
        parentHash: Uint8Array,
        block: StorageEosioDelta,
        evmTxs: StorageEosioAction[]
    ): IndexedBlockInfo {
        const evmBlockNum = block.block_num - this.config.evmBlockDelta;

        let receiptsHash = EMPTY_TRIE_BUF;
        if (block['@receiptsRootHash'])
            receiptsHash = hexStringToUint8Array(block['@receiptsRootHash']);

        let txsHash = EMPTY_TRIE_BUF;
        if (block['@transactionsRoot'])
            txsHash = hexStringToUint8Array(block['@transactionsRoot']);

        let gasUsed = BigInt(0);
        const blockBloom = new Bloom();
        for (const tx of evmTxs) {
            gasUsed += BigInt(tx['@raw'].gasused);
            if (tx['@raw'].logsBloom)
                blockBloom.or(new Bloom(hexStringToUint8Array(tx['@raw'].logsBloom)));
        }

        const blockHeader = TEVMBlockHeader.fromHeaderData({
            'parentHash': parentHash,
            'transactionsTrie': txsHash,
            'receiptTrie': receiptsHash,
            'stateRoot': EMPTY_TRIE_BUF,
            'logsBloom': blockBloom.bitvector,
            'number': BigInt(evmBlockNum),
            'gasLimit': BLOCK_GAS_LIMIT,
            'gasUsed': gasUsed,
            'timestamp': BigInt(moment.utc(block['@timestamp']).unix()),
            'extraData': hexStringToUint8Array(block['@blockHash'])
        }, {common: this.common});

        const currentBlockHash = blockHeader.hash();

        const storableActions: StorageEosioAction[] = [];
        if (evmTxs.length > 0) {
            for (const tx of evmTxs) {
                tx['@raw'].block_hash = arrayToHex(currentBlockHash);
                storableActions.push(tx);
            }
        }
        return {
            "transactions": storableActions,
            "errors": [],
            "delta": {
                "@timestamp": block['@timestamp'],
                "block_num": block.block_num,
                "@global": {
                    "block_num": evmBlockNum
                },
                "@evmPrevBlockHash": arrayToHex(parentHash),
                "@evmBlockHash": arrayToHex(currentBlockHash),
                "@blockHash": block['@blockHash'],
                "@receiptsRootHash": block['@receiptsRootHash'],
                "@transactionsRoot": block['@transactionsRoot'],
                "gasUsed": gasUsed.toString(),
                "gasLimit": BLOCK_GAS_LIMIT.toString(),
                "txAmount": storableActions.length,
                "size": block['size']
            },
            "nativeHash": block['@blockHash'],
            "parentHash": arrayToHex(parentHash),
            "receiptsRoot": block['@receiptsRootHash'],
            "blockBloom": arrayToHex(blockBloom.bitvector)
        };
    }

    async reindex(targetPrefix: string, opts: {
        eval?: boolean,
        timeout?: number,
        trimFrom?: number
    } = {}) {
        const config = cloneDeep(DEFAULT_CONF);
        mergeDeep(config, this.config);

        this.config = cloneDeep(config);

        config.chainName = targetPrefix;

        this.reindexConnector = new Connector(config, this.logger);
        await this.reindexConnector.init();

        if (opts.trimFrom)
            await this.reindexConnector.purgeNewerThan(opts.trimFrom);

        // for (const index of (await reindexConnector.getOrderedDeltaIndices()))
        //    await reindexConnector.elastic.indices.delete({index: index.index});

        const reindexLastBlock = await this.reindexConnector.getLastIndexedBlock();

        if (reindexLastBlock != null && reindexLastBlock.block_num < config.stopBlock) {
            config.startBlock = reindexLastBlock.block_num + 1;
            config.evmPrevHash = reindexLastBlock['@evmBlockHash'];
            config.evmValidateHash = '';
        }

        const totalBlocks = config.stopBlock - config.startBlock;

        this.logger.info(`starting reindex from ${config.startBlock} with prev hash \"${config.evmPrevHash}\"`);
        this.logger.info(`need to reindex ${totalBlocks.toLocaleString()} blocks total.`);

        const blockScroller = this.connector.blockScroll({
            from: config.startBlock,
            to: config.stopBlock,
            tag: `reindex-into-${targetPrefix}`,
            logLevel: process.env.SCROLL_LOG_LEVEL,
            scrollOpts: {
                fields: [
                    '@timestamp',
                    'block_num',
                    '@blockHash',
                    '@transactionsRoot',
                    '@receiptsRootHash',
                    'size',
                    'gasUsed'
                ],
                size: config.elastic.scrollSize,
                scroll: config.elastic.scrollWindow
            }
        });
        await blockScroller.init();

        const startTime = performance.now();
        let prevDeltaIndex = blockScroller.currentDeltaIndex;
        let prevActionIndex = blockScroller.currentActionIndex;
        const evalFn = async (srcBlock: BlockData, dstBlock: BlockData) => {
            const currentDeltaIndex = blockScroller.currentDeltaIndex;
            const currentActionIndex = blockScroller.currentActionIndex;
            if (prevDeltaIndex !== currentDeltaIndex) {
                // detect index change and compare document amounts
                const srcDeltaCount = await this.connector.getDocumentCountAtIndex(prevDeltaIndex);
                const dstDeltaCount = await this.connector.getDocumentCountAtIndex(prevDeltaIndex);
                expect(srcDeltaCount, 'expected delta count to match on index switch').to.be.equal(dstDeltaCount);prevDeltaIndex
                const srcActionCount = await this.connector.getDocumentCountAtIndex(prevActionIndex);
                const dstActionCount = await this.connector.getDocumentCountAtIndex(prevActionIndex);
                expect(srcActionCount, 'expected action count to match on index switch').to.be.equal(dstActionCount);
                prevDeltaIndex = currentDeltaIndex;
                prevActionIndex = currentActionIndex;
            }

            const srcDelta = srcBlock.block;
            const reindexDelta = dstBlock.block;

            expect(srcDelta.block_num).to.be.equal(reindexDelta.block_num);
            expect(srcDelta['@timestamp']).to.be.equal(reindexDelta['@timestamp']);
            expect(srcDelta['@blockHash']).to.be.equal(reindexDelta['@blockHash']);

            let gasUsed = srcDelta.gasUsed;
            if (!gasUsed)
                gasUsed = '0';
            expect(gasUsed).to.be.equal(reindexDelta.gasUsed);

            expect(srcBlock.actions.length).to.be.equal(dstBlock.actions.length);

            if ('txAmount' in srcBlock.block) {
                expect(srcBlock.block.txAmount).to.be.equal(dstBlock.block.txAmount);
                expect(srcBlock.block.txAmount).to.be.equal(srcBlock.actions.length);
            }

            srcBlock.actions.forEach((action, actionIndex) => {
                const reindexActionDoc = dstBlock.actions[actionIndex];
                const reindexAction = StorageEosioActionSchema.parse(reindexActionDoc);
                const srcAction = StorageEosioActionSchema.parse(action);
                srcAction['@raw'].block_hash = reindexActionDoc['@raw'].block_hash;

                expect(srcAction).to.be.deep.equal(reindexAction);
            });

            if (srcDelta.block_num % this.config.perf.elasticDumpSize != 0)
                return;

            const now = performance.now();
            const currentTimeElapsed = moment.duration(now - startTime, 'ms').humanize();

            const currentBlockNum = srcDelta.block_num;

            const checkedBlocksCount = currentBlockNum - config.startBlock;
            const progressPercent = (((checkedBlocksCount / totalBlocks) * 100).toFixed(2) + '%').padStart(6, ' ');
            const currentProgress = currentBlockNum - config.startBlock;

            this.logger.info('-'.repeat(32));
            this.logger.info('Reindex stats:');
            this.logger.info(`last checked  ${srcDelta.block_num.toLocaleString()}`);
            this.logger.info(`progress:     ${progressPercent}, ${currentProgress.toLocaleString()} blocks`);
            this.logger.info(`time elapsed: ${currentTimeElapsed}`);
            this.logger.info(`ETA:          ${moment.duration((totalBlocks - currentProgress) / metrics.average, 's').humanize()}`);
            this.logger.info('-'.repeat(32));
        };

        const metrics = new ThroughputMeasurer({windowSizeMs: 10 * 1000});
        let blocksPushed = 0;
        let lastPushed = 0;
        this.reindexPerfTaskId = setInterval(async () => {
            metrics.measure(blocksPushed);
            this.logger.info(`${lastPushed.toLocaleString()} @ ${metrics.average.toFixed(2)} blocks/s 10 sec avg`);
            blocksPushed = 0;
        }, 1000);

        const timeout = opts.timeout ? opts.timeout : undefined;
        const initialHash = config.evmPrevHash ? config.evmPrevHash : ZERO_HASH;
        let firstHash = '';
        let parentHash = hexStringToUint8Array(initialHash);

        this.events.emit('reindex-start');

        for await (const blockData of blockScroller) {
            const now = performance.now();
            const currentTimeElapsed = (now - startTime) / 1000;

            if (timeout && currentTimeElapsed > timeout) {
                this.logger.error('reindex timedout!');
                break;
            }

            const storableBlock = this.reindexBlock(parentHash, blockData.block, blockData.actions);

            if (opts.eval) {
                await evalFn(
                    blockData,
                    {block: storableBlock.delta, actions: storableBlock.transactions}
                );
            }

            if (firstHash === '') {
                firstHash = storableBlock.delta['@evmBlockHash'];
                if (config.evmValidateHash &&
                    firstHash !== config.evmValidateHash)
                    throw new Error(`initial hash validation failed: got ${firstHash} and expected ${config.evmValidateHash}`);
            }

            parentHash = hexStringToUint8Array(storableBlock.delta['@evmBlockHash']);
            await this.reindexConnector.pushBlock(storableBlock);

            lastPushed = blockData.block.block_num;
            blocksPushed++;
        }

        await this.reindexConnector.flush();
        clearInterval(this.reindexPerfTaskId);

        this.events.emit('reindex-stop');
    }

    /*
     * Wait until all db connector write tasks finish
     */
    async _waitWriteTasks() {
        if (!this.connector)
            return;

        while (this.connector.writeCounter > 0) {
            this.logger.debug(`waiting for ${this.connector.writeCounter} write operations to finish...`);
            await sleep(200);
        }
    }

    /*
     * Stop indexer gracefully
     */
    async stop() {
        clearInterval(this.perfTaskId as unknown as number);
        clearInterval(this.stateSwitchTaskId as unknown as number);

        if (this.reader) {
            try {
                await this.reader.stop();
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

        if (this.reindexConnector) {
            try {
                await this.reindexConnector.deinit();
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
        //     setTimeout(logWhyIsNodeRunning, 5000);
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
        this.logger.info(`Telos EVM Translator v${packageInfo.version}`);
        this.logger.info('Happy indexing!');
    }
}
