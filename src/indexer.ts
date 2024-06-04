import {readFileSync} from "node:fs";

import {HyperionSequentialReader, ThroughputMeasurer} from "@telosnetwork/hyperion-sequential-reader";
import {loggers, format, Logger, transports} from 'winston';
import {TEVMBlockHeader} from "telos-evm-custom-ds";
import {JsonRpc} from 'eosjs';
import * as evm from "@ethereumjs/common";
import cloneDeep from "lodash.clonedeep";
import {clearInterval} from "timers";
import {ABI} from "@greymass/eosio";
import workerpool from 'workerpool';
import moment from 'moment';

import {
    IndexedAccountDelta, IndexedAccountStateDelta,
    IndexedBlock, IndexedTx,
    IndexerState,
    StartBlockInfo,
} from './types/indexer.js';
import {
    arrayToHex,
    generateBlockApplyInfo,
    hexStringToUint8Array,
    isTxDeserializationError,
    BLOCK_GAS_LIMIT, EMPTY_TRIE_BUF, ZERO_HASH_BUF
} from './utils/evm.js'

import EventEmitter from "events";

import {packageInfo, prepareTranslatorConfig, sleep} from "./utils/indexer.js";
import {HandlerArguments} from "./workers/handlers.js";
import {getRPCClient} from './utils/eosio.js';
import {humanizeByteSize, mergeDeep} from "./utils/misc.js";
import {expect} from "chai";
import {Connector} from "./data/connector.js";
import {ElasticConnector} from "./data/elastic/elastic.js";
import {ArrowConnector} from "./data/arrow/arrow.js";
import {DecodedBlock} from "@telosnetwork/hyperion-sequential-reader/lib/esm/types/antelope";
import {ChainConfig, DEFAULT_CONF, TranslatorConfig} from "./types/config.js";
import {featureManager, initFeatureManager} from "./features.js";
import {Bloom} from "@ethereumjs/vm";

EventEmitter.defaultMaxListeners = 1000;

export class TEVMIndexer {
    state: IndexerState = IndexerState.SYNC;  // global indexer state, either HEAD or SYNC, changes buffered-writes-to-db machinery to be write-asap

    config: TranslatorConfig;  // global translator config as defined by environment or config file

    private readonly srcChain: ChainConfig;
    private readonly dstChain: Partial<ChainConfig>;

    private reader: HyperionSequentialReader;  // websocket state history connector, deserializes nodeos protocol
    private readonly readerAbis: {account: string, abi: ABI}[];

    private rpc: JsonRpc;
    private remoteRpc: JsonRpc;
    sourceConnector: Connector;  // custom elastic search db driver
    targetConnector: Connector;

    // private prevHash: string;  // previous indexed block evm hash, needed by machinery (do not modify manualy)
    headBlock: bigint;
    lastBlock: bigint;  // last block number that was succesfully pushed to db in order

    // debug status used to print statistics
    private pushedLastUpdate: number = 0;
    private stallCounter: number = 0;
    private perfMetrics: ThroughputMeasurer = new ThroughputMeasurer({
        windowSizeMs: 10 * 1000
    });

    private perfTaskId: NodeJS.Timer;
    private stateSwitchTaskId: NodeJS.Timer;

    private readonly logger: Logger;

    events = new EventEmitter();

    private evmDeserializationPool;

    private readonly common: evm.Common;
    private _isRestarting: boolean = false;
    private readonly srcCommon: evm.Common;
    private readonly dstCommon: evm.Common;

    private _mustStop: boolean = false;

    constructor(config: TranslatorConfig) {
        this.config = prepareTranslatorConfig(config);

        initFeatureManager(this.config.target.compatLevel);

        this.srcChain = this.config.source.chain;
        this.dstChain = this.config.target.chain;

        this.srcCommon = evm.Common.custom({
            chainId: this.srcChain.chainId,
            defaultHardfork: evm.Hardfork.Istanbul
        }, {baseChain: evm.Chain.Mainnet});

        this.dstCommon = evm.Common.custom({
            chainId: this.dstChain.chainId,
            defaultHardfork: evm.Hardfork.Istanbul
        }, {baseChain: evm.Chain.Mainnet});

        if (config.source.nodeos) {
            this.rpc = getRPCClient(config.source.nodeos.endpoint);
            this.remoteRpc = getRPCClient(config.source.nodeos.remoteEndpoint);
            this.readerAbis = ['eosio', 'eosio.token', 'eosio.msig', 'telos.evm'].map(abiName => {
                const jsonAbi = JSON.parse(readFileSync(`src/abis/${abiName}.json`).toString());
                return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
            });
        }

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
            ),
            transports: [
                new transports.Console({
                    level: this.config.logLevel
                })
            ]
        }
        this.logger = loggers.add('translator', loggingOptions);
        this.logger.debug('Logger initialized with level ' + this.config.logLevel);
    }

    /*
     * Debug routine that prints indexing stats, periodically called every second
     */
    async performanceMetricsTask() {

        if (this.config.source.nodeos && this.perfMetrics.max > 0) {
            if (this.perfMetrics.average == 0)
                this.stallCounter++;

            if (this.stallCounter > this.config.source.nodeos.stallCounter) {
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
        this.reader.restart(1000, Number(this.lastBlock) + 1);
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
    async hashBlock(block: Partial<IndexedBlock>): Promise<{
        evmBlockHash: Uint8Array,
        receiptsRoot: Uint8Array,
        txsRoot: Uint8Array
    }> {
        // generate block info derived from applying the transactions to the vm state
        const blockApplyInfo = await generateBlockApplyInfo(block.transactions);

        // generate 'valid' block header
        const blockHeader = TEVMBlockHeader.fromHeaderData({
            'parentHash': block.evmPrevHash,
            'stateRoot': EMPTY_TRIE_BUF,
            'transactionsTrie': blockApplyInfo.txsRootHash.root(),
            'receiptTrie': blockApplyInfo.receiptsTrie.root(),
            'logsBloom': blockApplyInfo.blockBloom.bitvector,
            'number': block.evmBlockNum,
            'gasLimit': BLOCK_GAS_LIMIT,
            'gasUsed': blockApplyInfo.gasUsed,
            'timestamp': block.timestamp,
            'extraData': block.blockHash
        }, {common: this.dstCommon});

        const currentBlockHash = blockHeader.hash();

        if (block.blockNum === BigInt(this.dstChain.startBlock)) {
            if (this.dstChain.evmValidateHash &&
                Buffer.compare(currentBlockHash, this.dstChain.evmValidateHash)) {
                this.logger.error(`Generated first block:\n${JSON.stringify(blockHeader, null, 4)}`);
                throw new Error(`initial hash validation failed: got ${currentBlockHash} and expected ${this.dstChain.evmValidateHash}`);
            }
        }

        return {
            evmBlockHash: blockHeader.hash(),
            receiptsRoot: blockApplyInfo.receiptsTrie.root(),
            txsRoot: blockApplyInfo.txsRootHash.root()
        }
    }

    private async handleStateSwitch() {
        if (!this.config.source.nodeos)
            throw new Error('handleStateSwitch task called but not reading from nodeos');

        // SYNC & HEAD mode switch detection
        try {
            this.headBlock = BigInt((await this.remoteRpc.get_info()).head_block_num);
            const isHeadTarget = this.headBlock >= this.srcChain.stopBlock;
            const targetBlock = isHeadTarget ? this.headBlock : BigInt(this.srcChain.stopBlock);

            const blocksUntilHead = Number(targetBlock - this.lastBlock);

            let statsString = `${blocksUntilHead.toLocaleString()} until target block ${targetBlock.toLocaleString()}`;
            if (this.perfMetrics.max != 0)
                statsString += ` ETA: ${moment.duration(blocksUntilHead / this.perfMetrics.average, 'seconds').humanize()}`;

            this.logger.info(statsString);

            if (isHeadTarget && blocksUntilHead <= 100) {
                if (this.state == IndexerState.HEAD)
                    return;

                this.state = IndexerState.HEAD;
                this.targetConnector.state = IndexerState.HEAD;

                this.logger.info(
                    'switched to HEAD mode! blocks will be written to db asap.');

            } else {
                if (this.state == IndexerState.SYNC)
                    return;

                this.state = IndexerState.SYNC;
                this.targetConnector.state = IndexerState.SYNC;

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
    async processSHIPBlock(prevHash: Uint8Array, block: DecodedBlock): Promise<IndexedBlock> {
        const currentBlock = BigInt(block.blockInfo.this_block.block_num);

        if (this._isRestarting) {
            this.logger.warn(`dropped ${currentBlock} due to restart...`);
            return;
        }

        if (currentBlock < this.srcChain.startBlock) {
            this.reader.ack();
            return;
        }

        if (this.srcChain.stopBlock > 0 && currentBlock > this.srcChain.stopBlock)
            return;

        if (currentBlock > this.lastBlock + 1n) {
            this.logger.warn(`Expected block ${this.lastBlock + 1n} and got ${currentBlock}, gap on reader?`);
            await this.resetReader();
            return;
        }

        this.stallCounter = 0;

        // native-evm block num delta is constant based on config
        const currentEvmBlock = currentBlock - BigInt(this.dstChain.evmBlockDelta);
        const blockTimestamp = BigInt(moment.utc(block.blockHeader.timestamp).unix());

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
        const accountDeltas: IndexedAccountDelta[] = [];
        const stateDeltas: IndexedAccountStateDelta[] = [];

        if (featureManager.isFeatureEnabled('STORE_ACC_DELTAS')) {
            let d = 0;
            block.deltas.forEach(delta => {
                const indexedDelta = {
                    timestamp: blockTimestamp,
                    blockNum: BigInt(currentBlock),
                    ordinal: d,
                    ...delta.value
                }
                if (delta.table == 'account')
                    accountDeltas.push(indexedDelta);

                if (delta.table == 'accountstate') {
                    indexedDelta.scope = delta.scope;
                    stateDeltas.push(indexedDelta);
                }

                d++;
            });
        }

        let evmTxs = [];
        const txTasks = [];
        const startTxTask = (taskType: string, params: HandlerArguments) => {
                txTasks.push(
                    this.evmDeserializationPool.exec(taskType, [params])
                        .catch((err) => {
                            this.logger.error(taskType);
                            this.logger.error(JSON.stringify(params, (_, v) => typeof v === 'bigint' ? v.toString() : v));
                            this.logger.error(err.message);
                            this.logger.error(err.stack);
                            throw err;
                        })
                );
        };
        const blockBloom = new Bloom();
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

            const actData: any = action.act.data;

            const isDeposit = isTransfer && actData.to === 'eosio.evm';

            // discard transfers to accounts other than eosio.evm
            // and transfers from system accounts
            if ((isTransfer && action.receiver != 'eosio.evm') ||
                (isTransfer && actData.from in systemAccounts))
                continue;

            const params: HandlerArguments = {
                nativeBlockHash: block.blockInfo.this_block.block_id,
                trx_index: txTasks.length,
                blockNum: BigInt(currentEvmBlock),
                tx: actData,
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

        evmTxs = await Promise.all(txTasks);

        const evmTransactions: IndexedTx[] = []

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

            const action = actions[i];

            evmTx.trxId = action.trxId;
            evmTx.actionOrdinal = action.actionOrdinal;

            gasUsedBlock += BigInt(evmTx.gasUsed);
            evmTx.gasUsedBlock = gasUsedBlock;

            blockBloom.or(new Bloom(evmTx.logsBloom));

            evmTransactions.push(evmTx);
            i++;
        }

        const indexedBlock: IndexedBlock = {
            timestamp: blockTimestamp,

            blockNum: currentBlock,
            blockHash: hexStringToUint8Array(block.blockInfo.this_block.block_id),

            evmBlockNum: currentEvmBlock,
            evmBlockHash: new Uint8Array(),
            evmPrevHash: prevHash,

            receiptsRoot: new Uint8Array(),
            transactionsRoot: new Uint8Array(),

            gasUsed: gasUsedBlock,
            gasLimit: BLOCK_GAS_LIMIT,

            size: 0n,

            transactionAmount: evmTransactions.length,
            transactions: evmTransactions,

            logsBloom: blockBloom.bitvector,

            deltas: {
                account: accountDeltas,
                accountstate: stateDeltas
            }
        };

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // fork handling
        if (currentBlock < this.lastBlock + 1n)
            indexedBlock.evmPrevHash = await this.handleFork(indexedBlock);

        const {
            evmBlockHash,
            receiptsRoot,
            txsRoot
        } = await this.hashBlock(indexedBlock);

        indexedBlock.evmBlockHash = evmBlockHash;
        indexedBlock.receiptsRoot = receiptsRoot;
        indexedBlock.transactionsRoot = txsRoot;

        if (this._isRestarting) {
            this.logger.warn(`dropped block ${currentBlock} due to restart...`);
            return;
        }

        // Update block num state tracking attributes
        this.lastBlock = currentBlock;

        // Push to db
        await this.targetConnector.pushBlock(indexedBlock);
        this.events.emit('push-block', indexedBlock);

        if (currentBlock == BigInt(this.srcChain.stopBlock)) {
            await this.stop();
            this.events.emit('stop');
            return;
        }

        // For debug stats
        this.pushedLastUpdate++;

        this.reader.ack();

        return indexedBlock;
    }

    async getOldHash(blockNum: bigint) {
        const block = await this.targetConnector.getIndexedBlock(blockNum);
        if(!block)
            throw new Error(`Block #${blockNum} not found in db`);
        return block.evmBlockHash;
    }

    async startReaderFrom(prevHash: Uint8Array, blockNum: bigint) {
        if (!this.config.source.nodeos)
            throw new Error('Tried to start reader but no nodeos config provided');

        const nodeos = this.config.source.nodeos;

        this.reader = new HyperionSequentialReader({
            shipApi: nodeos.wsEndpoint,
            chainApi: nodeos.endpoint,
            poolSize: nodeos.readerWorkerAmount,
            blockConcurrency: nodeos.readerWorkerAmount,
            blockHistorySize: nodeos.blockHistorySize,
            startBlock: Number(blockNum),
            endBlock: Number(this.srcChain.stopBlock),
            actionWhitelist: {
                'eosio.token': ['transfer'],
                'eosio.msig': ['exec'],
                'eosio.evm': ['raw', 'withdraw']
            },
            tableWhitelist: {
                'eosio.evm': ['account', 'accountstate']
            },
            irreversibleOnly: this.srcChain.irreversibleOnly,
            logLevel: (this.config.readerLogLevel || 'info').toLowerCase(),
            maxMsgsInFlight: nodeos.maxMsgsInFlight ?? 4000,
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

        this.reader.events.on('block', async (decodedBlock) => {
            const indexedBlock = await this.processSHIPBlock(prevHash, decodedBlock);
            prevHash = indexedBlock.evmBlockHash;
        });
        await this.reader.start();
    }

    newConnector(connConfig: any): Connector {
        if (connConfig.elastic)
            return new ElasticConnector(connConfig);
        else if (connConfig.arrow) {
            return new ArrowConnector(connConfig);
        } else
            throw new Error(
                'Could not figure out target, malformed config!\n'+
                `Check config-templates/ dir for examples.`
            );
    }

    /*
     * Entry point
     */
    async launch() {
        this.printIntroText();

        let startBlock = this.srcChain.startBlock;
        let prevHash: Uint8Array;

        this.targetConnector = this.newConnector(this.config.target);

        const gap = await this.targetConnector.init();

        if (this.config.runtime.onlyDBCheck) {
            this.logger.info('--only-db-check passed exiting...');
            await this.targetConnector.deinit();
            return;
        }

        if (this.srcChain.chainName !== this.dstChain.chainName) {
            await this.reindex();
            await this.targetConnector.deinit();
            return;
        }

        let lastBlock = await this.targetConnector.getLastIndexedBlock();

        if (lastBlock != null &&
            Buffer.compare(lastBlock.evmPrevHash, ZERO_HASH_BUF) != 0) {
            // if there is a last block found on db other than genesis doc

            if (gap == null) {
                ({startBlock, prevHash} = await this.getBlockInfoFromLastBlock(lastBlock));
            } else {
                if (this.config.target.gapsPurge)
                    ({startBlock, prevHash} = await this.getBlockInfoFromGap(gap));
                else
                    throw new Error(
                        `Gap found in database at ${gap}, but --gaps-purge flag not passed!`);
            }

        } else if (
            this.dstChain.evmPrevHash &&
            Buffer.compare(this.dstChain.evmPrevHash, ZERO_HASH_BUF) !== 0) {
            // if there is an evmPrevHash set state directly
            prevHash = this.dstChain.evmPrevHash;
        }

        if (prevHash)
            this.logger.info(`start from ${startBlock} with hash 0x${prevHash}.`);
        else {
            this.logger.info(`starting from genesis block ${startBlock}`);
            startBlock = (await this.genesisBlockInitialization()) + 1n;
            prevHash = EMPTY_TRIE_BUF;
        }

        this.srcChain.startBlock = startBlock;
        this.lastBlock = startBlock - 1n;
        this.targetConnector.lastPushed = this.lastBlock;
        this.dstChain.evmPrevHash = prevHash;

        if (this.config.source.nodeos) {
            const nodeosConfig = this.config.source.nodeos;
            if (!nodeosConfig.skipStartBlockCheck) {
                // check node actually contains first block
                try {
                    await this.rpc.get_block(startBlock.toString());
                } catch (error) {
                    throw new Error(
                        `Error when doing start block check: ${error.message}`);
                }
            }

            if (!nodeosConfig.skipRemoteCheck) {
                // check remote node is up
                try {
                    await this.remoteRpc.get_info();
                } catch (error) {
                    this.logger.error(`Error while doing remote node check: ${error.message}`);
                    throw error;
                }
            }

            process.env.CHAIN_ID = this.dstChain.chainId.toString();
            process.env.ENDPOINT = this.config.source.nodeos.endpoint;
            process.env.LOG_LEVEL = this.config.logLevel;

            process.env.COMPAT_TARGET = this.config.target.compatLevel;

            this.evmDeserializationPool = workerpool.pool(
                './build/workers/handlers.js', {
                    minWorkers: this.config.source.nodeos.evmWorkerAmount,
                    maxWorkers: this.config.source.nodeos.evmWorkerAmount,
                    workerType: 'thread'
                });
        }

        this.logger.info('Initializing ws broadcast...')
        this.targetConnector.startBroadcast(this.config.broadcast);

        await this.startReaderFrom(prevHash, BigInt(startBlock));

        // Launch bg routines
        this.perfTaskId = setInterval(async () => await this.performanceMetricsTask(), 1000);
        this.stateSwitchTaskId = setInterval(() => this.handleStateSwitch(), 10 * 1000);
    }

    async genesisBlockInitialization(): Promise<bigint> {
        const genesisBlock = await this.getGenesisBlock();

        // number of seconds since epoch
        const genesisTimestamp = moment.utc(genesisBlock.timestamp).unix();

        // genesis evm block num
        const genesisEvmBlockNum = BigInt(genesisBlock.block_num) - this.dstChain.evmBlockDelta;

        const genesisHeader = TEVMBlockHeader.fromHeaderData({
            'number': BigInt(genesisEvmBlockNum),
            'gasLimit': BLOCK_GAS_LIMIT,
            'stateRoot': EMPTY_TRIE_BUF,
            'timestamp': BigInt(genesisTimestamp),
            'extraData': hexStringToUint8Array(genesisBlock.id)
        }, {common: this.dstCommon});

        const genesisHash = genesisHeader.hash();

        if (this.dstChain.evmValidateHash &&
            Buffer.compare(this.dstChain.evmValidateHash, ZERO_HASH_BUF) !== 0  &&
            genesisHash != this.dstChain.evmValidateHash) {
            this.logger.error(`Generated genesis: \n${JSON.stringify(genesisHeader, null, 4)}`);
            throw new Error('FATAL!: Generated genesis hash doesn\'t match remote!');
        }

        // Init state tracking attributes
        const lastBlock = BigInt(genesisBlock.block_num);

        this.logger.info('ethereum genesis header: ');
        this.logger.info(JSON.stringify(genesisHeader.toJSON(), null, 4));

        this.logger.info(`ethereum genesis hash: 0x${arrayToHex(genesisHash)}`);

        // if we are starting from genesis store block skeleton doc
        // for rpc to be able to find parent hash for fist block
        await this.targetConnector.pushBlock({
            timestamp: BigInt(genesisTimestamp),

            blockNum: BigInt(genesisBlock.block_num),
            blockHash: hexStringToUint8Array(genesisBlock.id.toLowerCase()),

            evmBlockNum: genesisEvmBlockNum,
            evmBlockHash: genesisHash,
            evmPrevHash: ZERO_HASH_BUF,

            receiptsRoot: EMPTY_TRIE_BUF,
            transactionsRoot: EMPTY_TRIE_BUF,

            gasUsed: 0n,
            gasLimit: BLOCK_GAS_LIMIT,

            size: 0n,

            transactionAmount: 0,

            transactions: [],

            logsBloom: (new Bloom()).bitvector,

            deltas: {
                account: [],
                accountstate: []
            }
        })

        this.events.emit('start');

        return lastBlock;
    }

    private async reindexBlock(parentHash: Uint8Array, block: IndexedBlock): Promise<IndexedBlock> {
        const evmBlockNum = block.blockNum - BigInt(this.dstChain.evmBlockDelta);

        const reindexedBlock = cloneDeep(block);

        reindexedBlock.evmBlockNum = evmBlockNum;
        reindexedBlock.evmPrevHash = parentHash;

        const {
            evmBlockHash,
            receiptsRoot,
            txsRoot
        } = await this.hashBlock(reindexedBlock);

        reindexedBlock.evmBlockHash = evmBlockHash;
        reindexedBlock.receiptsRoot = receiptsRoot;
        reindexedBlock.transactionsRoot = txsRoot;

        return reindexedBlock;
    }

    async reindex() {
        const config = cloneDeep(DEFAULT_CONF);
        mergeDeep(config, this.config);

        this.sourceConnector = this.newConnector(config.source);
        await this.sourceConnector.init();

        const reindexLastBlock = await this.targetConnector.getLastIndexedBlock();

        if (reindexLastBlock != null && reindexLastBlock.blockNum < this.dstChain.stopBlock) {
            this.dstChain.startBlock = reindexLastBlock.blockNum + 1n;
            this.dstChain.evmPrevHash = reindexLastBlock.evmBlockHash;
            this.dstChain.evmValidateHash = ZERO_HASH_BUF;
        }

        const totalBlocks = this.dstChain.stopBlock - this.dstChain.startBlock;

        this.logger.info(`starting reindex from ${this.dstChain.startBlock} with prev hash \"${this.dstChain.evmPrevHash}\"`);
        this.logger.info(`need to reindex ${totalBlocks.toLocaleString()} blocks total.`);

        let scrollOpts = {};
        let batchSize: number = 1000;
        if (this.config.source.elastic) {
            const esconfig = this.config.source.elastic;
            scrollOpts = {
                fields: [
                    '@timestamp',
                    'block_num',
                    '@blockHash',
                    '@transactionsRoot',
                    '@receiptsRootHash',
                    'size',
                    'gasUsed'
                ],
                size: esconfig.scrollSize,
                scroll: esconfig.scrollWindow
            };
            batchSize = esconfig.scrollSize;
        }

        const blockScroller = this.sourceConnector.blockScroll({
            from: this.dstChain.startBlock,
            to: this.dstChain.stopBlock,
            tag: `reindex-into-${this.dstChain.chainName}`,
            logLevel: process.env.SCROLL_LOG_LEVEL,
            scrollOpts
        });
        await blockScroller.init();

        const startTime = performance.now();
        // let prevDeltaIndex = blockScroller.currentDeltaIndex;
        // let prevActionIndex = blockScroller.currentActionIndex;
        const evalFn = async (srcBlock: IndexedBlock, dstBlock: IndexedBlock) => {
            // const currentDeltaIndex = blockScroller.currentDeltaIndex;
            // const currentActionIndex = blockScroller.currentActionIndex;
            // if (prevDeltaIndex !== currentDeltaIndex) {
            //     // detect index change and compare document amounts
            //     const srcDeltaCount = await this.connector.getDocumentCountAtIndex(prevDeltaIndex);
            //     const dstDeltaCount = await this.connector.getDocumentCountAtIndex(prevDeltaIndex);
            //     expect(srcDeltaCount, 'expected delta count to match on index switch').to.be.equal(dstDeltaCount);prevDeltaIndex
            //     const srcActionCount = await this.connector.getDocumentCountAtIndex(prevActionIndex);
            //     const dstActionCount = await this.connector.getDocumentCountAtIndex(prevActionIndex);
            //     expect(srcActionCount, 'expected action count to match on index switch').to.be.equal(dstActionCount);
            //     prevDeltaIndex = currentDeltaIndex;
            //     prevActionIndex = currentActionIndex;
            // }

            expect(srcBlock.blockNum).to.be.equal(dstBlock.blockNum);
            expect(srcBlock.timestamp).to.be.equal(dstBlock.timestamp);
            expect(srcBlock.blockHash).to.be.equal(dstBlock.blockHash);

            let gasUsed = srcBlock.gasUsed;
            expect(gasUsed).to.be.equal(dstBlock.gasUsed);

            expect(srcBlock.transactions.length).to.be.equal(dstBlock.transactions.length);
            expect(srcBlock.transactionAmount).to.be.equal(dstBlock.transactionAmount);

            srcBlock.transactions.forEach((action, actionIndex) => {
                const reindexAction = dstBlock.transactions[actionIndex];
                action.blockHash = reindexAction.blockHash;

                expect(action).to.be.deep.equal(reindexAction);
            });

            if (srcBlock.blockNum % BigInt(batchSize) != 0n)
                return;

            const now = performance.now();
            const currentTimeElapsed = moment.duration(now - startTime, 'ms').humanize();

            const currentBlockNum = srcBlock.blockNum;

            const checkedBlocksCount = currentBlockNum - this.dstChain.startBlock;
            const progressPercent = (((Number(checkedBlocksCount) / Number(totalBlocks)) * 100).toFixed(2) + '%').padStart(6, ' ');
            const currentProgress = currentBlockNum - this.dstChain.startBlock;

            const memStats = process.memoryUsage();

            this.logger.info('-'.repeat(32));
            this.logger.info('Reindex stats:');
            this.logger.info(`last checked  ${srcBlock.blockNum.toLocaleString()}`);
            this.logger.info(`progress:     ${progressPercent}, ${currentProgress.toLocaleString()} blocks`);
            this.logger.info(`time elapsed: ${currentTimeElapsed}`);
            this.logger.info(`ETA:          ${moment.duration(Number(totalBlocks - currentProgress) / this.perfMetrics.average, 's').humanize()}`);
            this.logger.info('memory stats:');
            this.logger.info(`total:          ${humanizeByteSize(memStats['rss'])}`);
            this.logger.info(`heap:           ${humanizeByteSize(memStats['heapTotal'])}`);
            this.logger.info(`buffers:        ${humanizeByteSize(memStats['arrayBuffers'])}`);
            this.logger.info(`external:       ${humanizeByteSize(memStats['external'])}`);
            this.logger.info('-'.repeat(32));
        };

        const initialHash = this.dstChain.evmPrevHash ? this.dstChain.evmPrevHash : ZERO_HASH_BUF;
        let firstHash = undefined;
        let parentHash = initialHash;

        this.perfTaskId = setInterval(() => this.performanceMetricsTask(), 1000);
        this.events.emit('reindex-start');

        this.lastBlock = this.dstChain.startBlock - 1n;
        for await (const srcBlock of blockScroller) {
            const now = performance.now();
            const currentTimeElapsed = (now - startTime) / 1000;

            if (this.config.runtime.timeout && currentTimeElapsed > this.config.runtime.timeout) {
                this.logger.error('reindex timedout!');
                break;
            }

            if (this._mustStop)
                break;

            const dstBlock = await this.reindexBlock(parentHash, srcBlock);

            if (this.config.runtime.eval) {
                await evalFn(
                    srcBlock,
                    dstBlock
                );
            }

            if (typeof firstHash === 'undefined') {
                firstHash = dstBlock.evmBlockHash;
                if (this.dstChain.evmValidateHash &&
                    firstHash !== this.dstChain.evmValidateHash)
                    throw new Error(`initial hash validation failed: got ${arrayToHex(firstHash)} and expected ${arrayToHex(this.dstChain.evmValidateHash)}`);
            }

            parentHash = dstBlock.evmBlockHash;
            await this.targetConnector.pushBlock(dstBlock);
            this.lastBlock = dstBlock.blockNum;
            this.pushedLastUpdate++;
            this.perfMetrics.measure(this.pushedLastUpdate);
        }

        clearInterval(this.perfTaskId as unknown as number);
        await this.targetConnector.flush();
        this.events.emit('reindex-stop');
    }

    /*
     * Stop indexer gracefully
     */
    async stop() {
        this._mustStop = true;
        clearInterval(this.perfTaskId as unknown as number);
        clearInterval(this.stateSwitchTaskId as unknown as number);

        if (this.reader) {
            try {
                await this.reader.stop();
            } catch (e) {
                this.logger.warn(`error stopping reader: ${e.message}`);
            }
        }

        if (this.targetConnector) {
            try {
                await this.targetConnector.deinit();
            } catch (e) {
                this.logger.warn(`error stopping connector: ${e.message}`);
            }
        }

        if (this.sourceConnector) {
            try {
                await this.sourceConnector.deinit();
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
                    (this.srcChain.startBlock - 1n).toString());

            } catch (e) {
                this.logger.error(e);
                this.logger.warn(`couldn\'t get genesis block ${(this.srcChain.startBlock - 1n).toLocaleString()} retrying in 5 sec...`);
                await sleep(5000);
            }
        }
        return genesisBlock;
    }

    /*
     * Get start parameters from last block indexed on db
     */
    private async getBlockInfoFromLastBlock(lastBlock: IndexedBlock): Promise<StartBlockInfo> {

        // sleep, then get last block again, if block_num changes it means
        // another indexer is running
        await sleep(3000);
        const newlastBlock = await this.targetConnector.getLastIndexedBlock();
        if (lastBlock.blockNum != newlastBlock.blockNum)
            throw new Error(
                'New last block check failed probably another indexer is running, abort...');

        let startBlock = lastBlock.blockNum;

        this.logger.info('done.');

        lastBlock = await this.targetConnector.getLastIndexedBlock();

        const prettyTS = moment.utc(Number(lastBlock.timestamp)).toISOString();
        const prevHash = lastBlock.evmPrevHash;

        this.logger.info(
            `found! ${lastBlock.blockNum.toLocaleString()} produced on ${prettyTS} with hash 0x${arrayToHex(prevHash)}`)

        return {startBlock, prevHash};
    }

    /*
     * Get start parameters from first gap on database
     */
    private async getBlockInfoFromGap(gap: bigint): Promise<StartBlockInfo> {

        let firstBlock: IndexedBlock;
        let delta = 0n;
        while (!firstBlock || firstBlock.blockNum === undefined) {
            firstBlock = await this.targetConnector.getIndexedBlock(gap - delta);
            delta++;
        }
        // found blocks on the database
        this.logger.info(`Last block of continuous range found: ${JSON.stringify(firstBlock, null, 4)}`);

        let startBlock = firstBlock.blockNum;

        this.logger.info(`purge blocks newer than ${startBlock}`);

        await this.targetConnector.purgeNewerThan(startBlock);

        this.logger.info('done.');

        const lastBlock = await this.targetConnector.getLastIndexedBlock();

        let prevHash = lastBlock['@evmBlockHash'];

        if (lastBlock.blockNum != (startBlock - 1n))
            throw new Error(`Last block: ${lastBlock.blockNum.toLocaleString()}, is not ${(startBlock - 1n).toLocaleString()} - 1`);

        this.logger.info(
            `found! ${lastBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`)

        return {startBlock, prevHash};
    }

    /*
     * Handle fork, leave every state tracking attribute in a healthy state
     */
    private async handleFork(b: IndexedBlock): Promise<Uint8Array> {
        const lastNonForked = b.blockNum - 1n;
        const forkedAt = this.lastBlock;

        this.logger.info(`got ${b.blockNum.toLocaleString()} and expected ${(this.lastBlock + 1n).toLocaleString()}, chain fork detected. reverse all blocks which were affected`);

        await this.targetConnector.flush();

        // finally purge db
        await this.targetConnector.purgeNewerThan(lastNonForked + 1n);
        this.logger.debug(`purged db of blocks newer than ${lastNonForked}, continue...`);

        // tweak variables used by ordering machinery
        const newPrevHash = await this.getOldHash(lastNonForked);
        this.lastBlock = lastNonForked;

        this.targetConnector.forkCleanup(
            b.timestamp,
            lastNonForked,
            forkedAt
        );

        return newPrevHash;
    }

    printIntroText() {
        this.logger.info(`Telos EVM Translator v${packageInfo.version}`);
        this.logger.info('Happy indexing!');
    }
}
