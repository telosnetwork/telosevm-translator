import RPCBroadcaster from '../publisher.js';
import {
    IndexedAccountDelta,
    IndexedAccountStateDelta,
    IndexedBlock, IndexedBlockHeader, IndexedTx,
    IndexerState,
} from '../types/indexer.js';

import {createLogger, format, Logger, transports} from "winston";
import EventEmitter from "events";
import {BroadcasterConfig, ConnectorConfig} from "../types/config.js";


export abstract class BlockScroller {

    get isInit(): boolean {
        return this._isInit;
    }
    get isDone(): boolean {
        return this._isDone;
    }

    protected _isInit: boolean;
    protected _isDone: boolean;

    protected from: bigint;               // will push blocks >= `from`
    protected to: bigint;                 // will stop pushing blocks when `to` is reached
    protected validate: boolean;          // perform schema validation on docs read from source index

    tag: string;                        // tag scroller, usefull when using multiple to tell them apart on logs

    protected logger: Logger;

    protected connector: Connector;

    constructor(
        connector: Connector,
        params: {
            from: bigint,
            to: bigint,
            tag: string
            logLevel?: string,
            validate?: boolean
        }
    ) {
        this.connector = connector;
        this.from = params.from;
        this.to = params.to;
        this.tag = params.tag;
        this.validate = params.validate ? params.validate : false;
    }

    abstract init(): Promise<void>;
    abstract nextResult(): Promise<IndexedBlock>;

    /*
     * Important before using this in a for..of statement,
     * call this.init! class gets info about indexes needed
     * for scroll from elastic
     */
    async *[Symbol.asyncIterator](): AsyncIterableIterator<IndexedBlock> {
        do {
            const block = await this.nextResult();
            yield block;
        } while (!this._isDone)
    }
}

export abstract class Connector {
    config: ConnectorConfig;
    logger: Logger;
    chainName: string;
    state: IndexerState;

    totalPushed: bigint = 0n;
    lastPushed: bigint = 0n;

    broadcast: RPCBroadcaster;
    isBroadcasting: boolean = false;

    events = new EventEmitter();

    protected constructor(config: ConnectorConfig) {
        this.config = config;
        this.chainName = config.chain.chainName;

        const logLevel = config.logLevel ? config.logLevel : 'info';
        const loggingOptions = {
            exitOnError: false,
            level: logLevel,
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
            level: logLevel
        }));
    }

    async init(): Promise<bigint | null> {
        if (typeof this.config.trimFrom === 'number') {
            const trimBlockNum = BigInt(this.config.trimFrom);
            await this.purgeNewerThan(trimBlockNum);
        }

        this.logger.info('checking db for blocks...');
        let lastBlock = await this.getLastIndexedBlock();

        let gap = null;
        if (!this.config.skipIntegrityCheck) {
            if (lastBlock != null) {
                this.logger.debug('performing integrity check...');
                gap = await this.fullIntegrityCheck();

                if (gap == null) {
                    this.logger.info('NO GAPS FOUND');
                } else {
                    this.logger.info('GAP INFO:');
                    this.logger.info(JSON.stringify(gap, null, 4));
                }
            }
        }

        return gap;
    }

    startBroadcast(config: BroadcasterConfig) {
        this.broadcast = new RPCBroadcaster(config, this.logger);
        this.broadcast.initUWS();
        this.isBroadcasting = true;
    }

    stopBroadcast() {
        this.broadcast.close();
        this.isBroadcasting = false;
    }

    async deinit() {
        await this.flush();

        if (this.isBroadcasting)
            this.stopBroadcast();
    }

    abstract getTransactionsForBlock(blockNum: bigint) : Promise<IndexedTx[]>;

    abstract getAccountDeltasForBlock(blockNum: bigint): Promise<IndexedAccountDelta[]>;

    abstract getAccountStateDeltasForBlock(blockNum: bigint): Promise<IndexedAccountStateDelta[]>;

    abstract getBlockHeader(blockNum: bigint) : Promise<IndexedBlockHeader | null>

    abstract getIndexedBlock(blockNum: bigint) : Promise<IndexedBlock | null>;

    abstract getFirstIndexedBlock() : Promise<IndexedBlock | null>;

    abstract getLastIndexedBlock() : Promise<IndexedBlock | null>;

    async getBlockRange(from: bigint, to: bigint): Promise<IndexedBlock[]> {
        const blocks: IndexedBlock[] = [];
        const scroll = this.blockScroll({from, to, tag: 'get-block-range'});
        await scroll.init();
        for await (const block of scroll)
            blocks.push(block);
        return blocks;
    }

    abstract fullIntegrityCheck(): Promise<bigint | null>;

    abstract purgeNewerThan(blockNum: bigint) : Promise<void>;

    abstract flush() : Promise<void>;

    abstract pushBlock(blockInfo: IndexedBlock): Promise<void>;

    abstract forkCleanup(
        timestamp: bigint,
        lastNonForked: bigint,
        lastForked: bigint
    ): void;

    abstract blockScroll(params: {
        from: bigint,
        to: bigint,
        tag: string,
        logLevel?: string,
        validate?: boolean,
        scrollOpts?: any
    }) : BlockScroller;
}
