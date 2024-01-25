import RPCBroadcaster from '../publisher.js';
import {
    BroadcasterConfig,
    ConnectorConfig,
    IndexedBlockInfo,
    IndexerState,
} from '../types/indexer.js';

import {
    StorageEosioAction, StorageEosioActionSchema,
    StorageEosioDelta, StorageEosioGenesisDeltaSchema,
} from '../types/evm.js';
import {createLogger, format, Logger, transports} from "winston";
import EventEmitter from "events";
import {ElasticConnector, ScrollOptions} from "./elastic";


export interface BlockData {block: StorageEosioDelta, actions: StorageEosioAction[]};

export abstract class BlockScroller {

    get isInit(): boolean {
        return this._isInit;
    }
    get isDone(): boolean {
        return this._isDone;
    }

    protected _isInit: boolean;
    protected _isDone: boolean;

    protected from: number;               // will push blocks >= `from`
    protected to: number;                 // will stop pushing blocks when `to` is reached
    protected validate: boolean;          // perform schema validation on docs read from source index

    tag: string;                        // tag scroller, usefull when using multiple to tell them apart on logs

    protected logger: Logger;

    abstract init(): Promise<void>;
    abstract nextResult(): Promise<BlockData>;

    abstract [Symbol.asyncIterator](): AsyncIterableIterator<BlockData>;
}

export abstract class Connector {
    config: ConnectorConfig;
    logger: Logger;
    chainName: string;
    state: IndexerState;

    totalPushed: number = 0;
    lastPushed: number = 0;

    broadcast: RPCBroadcaster;
    isBroadcasting: boolean = false;

    events = new EventEmitter();

    protected constructor(config: ConnectorConfig, logger: Logger) {
        this.config = config;
        this.logger = logger;
        this.chainName = config.chain.chainName;
    }

    async init(): Promise<number | null> {
        if (this.config.trimFrom) {
            const trimBlockNum = this.config.trimFrom;
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

    abstract getIndexedBlock(blockNum: number) : Promise<StorageEosioDelta>;

    abstract getFirstIndexedBlock() : Promise<StorageEosioDelta>;

    abstract getLastIndexedBlock() : Promise<StorageEosioDelta>;

    abstract getBlockRange(from: number, to: number): Promise<BlockData[]>;

    abstract fullIntegrityCheck(): Promise<number>;

    abstract purgeNewerThan(blockNum: number) : Promise<void>;

    abstract flush() : Promise<void>;

    abstract pushBlock(blockInfo: IndexedBlockInfo): Promise<void>;

    abstract forkCleanup(
        timestamp: string,
        lastNonForked: number,
        lastForked: number
    ): void;

    abstract blockScroll(params: {
        from: number,
        to: number,
        tag: string,
        logLevel?: string,
        validate?: boolean,
        scrollOpts?: any
    }) : BlockScroller;
}
