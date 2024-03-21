import {
    ConnectorConfig,
    IndexedBlockInfo,
    ArrowConnectorConfig,
    IndexedAccountDelta,
    IndexedAccountStateDelta
} from "../../types/indexer.js";
import {BlockData, BlockScroller, Connector} from "../connector.js";
import {BLOCK_GAS_LIMIT, EMPTY_TRIE} from "../../utils/evm.js";
import {InternalEvmTransaction, StorageEosioAction, StorageEosioDelta} from "../../types/evm.js";

import moment from "moment";

import {
    Table,
} from 'apache-arrow';

import {ArrowBatchContext} from "./batch.js";
import {Name} from "@greymass/eosio";
import {ArrowTableMapping} from "./protocol.js";


export const translatorDataContext: {[key: string]: ArrowTableMapping[]} = {
    'block': [
        {name: 'timestamp',           type: 'u64'},
        {name: 'block_num',           type: 'u64'},
        {name: 'block_hash',          type: 'checksum256'},
        {name: 'evm_block_hash',      type: 'checksum256'},
        {name: 'evm_prev_block_hash', type: 'checksum256'},
        {name: 'receipts_hash',       type: 'checksum256'},
        {name: 'txs_hash',            type: 'checksum256'},
        {name: 'gas_used',            type: 'uintvar'},
        {name: 'txs_amount',          type: 'u32'},
        {name: 'size',                type: 'u32'},
    ],
    'itx': [
        {name: 'tx_index',   type: 'u64'},
        {name: 'ordinal',    type: 'u32'},
        {name: 'call_type',  type: 'bytes'},
        {name: 'from',       type: 'checksum160'},
        {name: 'gas',        type: 'uintvar'},
        {name: 'input',      type: 'bytes'},
        {name: 'to',         type: 'bytes'},
        {name: 'value',      type: 'uintvar'},
        {name: 'gas_used',   type: 'uintvar'},
        {name: 'output',     type: 'bytes'},
        {name: 'subtraces',  type: 'u8'},
        {name: 'type',       type: 'string'},
        {name: 'depth',      type: 'string'}
    ],
    'tx_log': [
        {name: 'tx_index',   type: 'u64'},
        {name: 'ordinal',    type: 'u32'},
        {name: 'address',   type: 'checksum160', optional: true},
        {name: 'data',      type: 'bytes', optional: true},
        {name: 'topics',    type: 'checksum256', array: true, optional: true},
    ],
    'tx': [
        {name: 'id',             type: 'checksum256'},
        {name: 'global_index',   type: 'u64'},
        {name: 'block_num',      type: 'u64'},
        {name: 'action_ordinal', type: 'u32'},

        {name: 'raw',            type: 'bytes'},
        {name: 'hash',           type: 'checksum256'},
        {name: 'from',           type: 'checksum160', optional: true},
        {name: 'evm_ordinal',    type: 'u32'},
        {name: 'block_hash',     type: 'checksum256'},
        {name: 'to',             type: 'bytes', optional: true},
        {name: 'input',          type: 'bytes'},
        {name: 'value',          type: 'uintvar'},
        {name: 'nonce',          type: 'uintvar'},
        {name: 'gas_price',      type: 'uintvar'},
        {name: 'gas_limit',      type: 'uintvar'},
        {name: 'status',         type: 'u8'},
        {name: 'itx_amount',     type: 'u32'},
        {name: 'epoch',          type: 'u32'},
        {name: 'created_addr',   type: 'checksum160', optional: true},
        {name: 'gas_used',       type: 'uintvar'},
        {name: 'gas_used_block', type: 'uintvar'},
        {name: 'charged_price',  type: 'uintvar'},
        {name: 'output',         type: 'bytes'},
        {name: 'logs_amount',    type: 'u32'},
        {name: 'logs_bloom',     type: 'bytes', optional: true},
        {name: 'errors',         type: 'string', optional: true, array: true},
        {name: 'v',              type: 'uintvar'},
        {name: 'r',              type: 'uintvar'},
        {name: 's',              type: 'uintvar'}
    ],
    'account': [
        {name: 'block_num', type: 'u64'},
        {name: 'ordinal',   type: 'u32'},
        {name: 'index',     type: 'u64'},
        {name: 'address',   type: 'checksum160'},
        {name: 'account',   type: 'u64'},
        {name: 'nonce',     type: 'u64'},
        {name: 'code',      type: 'bytes'},
        {name: 'balance',   type: 'bytes', length: 32},
    ],
    'accountstate': [
        {name: 'block_num', type: 'u64'},
        {name: 'ordinal',   type: 'u32'},
        {name: 'index',     type: 'u64'},
        {name: 'key',       type: 'bytes', length: 32},
        {name: 'value',     type: 'bytes', length: 32}
    ]
};


export class ArrowConnector extends Connector {

    readonly pconfig: ArrowConnectorConfig;

    private firstBlock: StorageEosioDelta;
    private lastBlock: StorageEosioDelta;
    private writer: ArrowBatchContext;

    private globalTxIndex: bigint = BigInt(0);

    constructor(config: ConnectorConfig) {
        super(config);

        if (!config.arrow)
            throw new Error(`Tried to init polars connector with null config`);

        this.pconfig = config.arrow;

        this.writer = new ArrowBatchContext(
            config.arrow,
            translatorDataContext,
            this.logger
        );
    }

    private addBlockRow(delta: StorageEosioDelta) {
        let receiptHash = EMPTY_TRIE;
        if (delta["@receiptsRootHash"])
            receiptHash = delta["@receiptsRootHash"];

        let txsHash = EMPTY_TRIE;
        if (delta["@transactionsRoot"])
            txsHash = delta["@transactionsRoot"];

        let gasUsed = 0;
        if (delta.gasUsed)
            gasUsed = parseInt(delta.gasUsed, 10);

        this.writer.addRow(
            'block',
            [
                moment.utc(delta["@timestamp"]).unix(),
                delta.block_num,
                delta["@blockHash"],
                delta["@evmBlockHash"],
                delta['@evmPrevBlockHash'],
                receiptHash,
                txsHash,
                gasUsed,
                delta.txAmount,
                parseInt(delta.size, 10)
            ]
        );
    }

    private addItxRow(
        ordinal: number,
        itx: InternalEvmTransaction
    ) {
        this.writer.addRow(
          'itx',
          [
              this.globalTxIndex,
              ordinal,
              itx.callType,
              itx.from,
              itx.gas,
              itx.input,
              itx.to,
              itx.value,
              itx.gasUsed,
              itx.output,
              itx.subtraces,
              itx.type,
              itx.depth
          ]
        );
    }

    private addTxLogRow(
        ordinal: number,
        log: {
            address?: string;
            topics?: string[];
            data?: string;
        }
    ) {
        this.writer.addRow(
            'tx_log',
            [
                this.globalTxIndex,
                ordinal,
                log.address,
                log.topics,
                log.data
            ]
        );
    }

    private addTxRow(
        blockNum: number,
        tx: StorageEosioAction
    ) {
        const evm_tx = tx['@raw'];
        const itxs = evm_tx.itxs ?? [];
        const logs = evm_tx.logs ?? [];
        this.writer.addRow(
            'tx',
            [
                tx.trx_id,
                this.globalTxIndex,
                blockNum,
                tx.action_ordinal,

                evm_tx.raw,
                evm_tx.hash,
                evm_tx.from,
                evm_tx.trx_index,
                evm_tx.block_hash,
                evm_tx.to,
                evm_tx.input_data,
                evm_tx.value,
                evm_tx.nonce,
                evm_tx.gas_price,
                evm_tx.gas_limit,
                evm_tx.status,
                itxs.length,
                evm_tx.epoch,
                evm_tx.createdaddr,
                evm_tx.gasused,
                evm_tx.gasusedblock,
                evm_tx.charged_gas_price,
                evm_tx.output,
                logs.length,
                evm_tx.logsBloom,
                evm_tx.errors,
                evm_tx.v, evm_tx.r, evm_tx.s
            ]
        );

        if (this.config.compatLevel.mayor == 1)
            itxs.forEach(
                (itx, index) => this.addItxRow(index, itx));

        logs.forEach(
            (log, index) => this.addTxLogRow(index, log));

        this.globalTxIndex++;
    }

    private addAccountRow(delta: IndexedAccountDelta) {
        this.writer.addRow(
            'account',
            [
                delta.block_num,
                delta.ordinal,
                delta.index,
                delta.address,
                Name.from(delta.account).value.toString(),
                delta.nonce,
                delta.code,
                delta.balance
            ]
        );
    }

    private addAccountStateRow(delta: IndexedAccountStateDelta) {
        this.writer.addRow(
            'accountstate',
            [
                delta.block_num,
                delta.ordinal,
                delta.index,
                delta.key,
                delta.value
            ]
        );
    }

    deltaFromTable(table: Table, index: number): StorageEosioDelta {
        const row = table.get(index).toArray();
        return {
            '@timestamp': new Date(parseInt(row[0].toString(10), 10)).toISOString(),
            block_num: parseInt(row[1].toString(10), 10),
            '@global': {
                block_num: parseInt(row[1].toString(10), 10) - this.config.chain.evmBlockDelta
            },
            '@blockHash': row[2],
            '@evmBlockHash': row[3],
            '@evmPrevBlockHash': row[4],
            '@receiptsRootHash': row[5],
            '@transactionsRoot': row[6],
            gasUsed: row[7].toString(),
            gasLimit: BLOCK_GAS_LIMIT.toString(),
            txAmount: row[8],
            size: String(row[9])
        }
    }

    async init(): Promise<number | null> {
        await this.writer.init(
            this.config.chain.startBlock);
        return await super.init();
    }

    async getBlockRange(from: number, to: number): Promise<BlockData[]> {
        return [];
    }

    async getFirstIndexedBlock(): Promise<StorageEosioDelta | null> {
        if (!this.firstBlock) return null;

        return this.firstBlock;
    }

    async getIndexedBlock(blockNum: number): Promise<StorageEosioDelta | null> {
        throw new Error(`Unimplemented`);
    }

    async getLastIndexedBlock(): Promise<StorageEosioDelta> {
        if (!this.lastBlock) return null;

        return this.lastBlock;
    }

    private purgeMemoryFrom(blockNum: number) {
        // purge on mem
        // const bucketStartNum = this._currentWriteBucket * this.pconfig.bucketSize;
        // const targetIndex = blockNum - bucketStartNum;

        throw new Error(`unimplemented`);
        // for (let i = 0; i < 10; i++)
        //     this._blocks.columns[i].values = this._blocks.columns[i].values.slice(0, targetIndex);
    }

    async purgeNewerThan(blockNum: number): Promise<void> {
        // purge on disk
        // await this.reloadOnDiskBuckets();

        // const deleteList = [];
        // for (const bucket of this._blockBuckets)
        //     if (this._bucketNameToNum(bucket) > this._adjustBlockNum(blockNum))
        //         deleteList.push(bucket);

        // for (const bucket of this._txBuckets)
        //     if (this._bucketNameToNum(bucket) > this._adjustBlockNum(blockNum))
        //         deleteList.push(bucket);

        // await Promise.all(deleteList.map(b => this.removeBucket(b)));

        // this.purgeMemoryFrom(blockNum);
        throw new Error(`unimplemented`);
    }

    async fullIntegrityCheck(): Promise<number | null> { return null; }

    async flush(): Promise<void> {}

    async pushBlock(blockInfo: IndexedBlockInfo): Promise<void> {
        this.lastBlock = blockInfo.block;
        this.lastPushed = this.lastBlock.block_num;

        this.addBlockRow(this.lastBlock);

        blockInfo.transactions.forEach(
            tx => this.addTxRow(this.lastPushed, tx));

        if (this.config.compatLevel.mayor >= 2) {
            blockInfo.deltas.account.forEach(
                accountDelta => this.addAccountRow(accountDelta));

            blockInfo.deltas.accountstate.forEach(
                stateDelta => this.addAccountStateRow(stateDelta));
        }

        this.writer.updateOrdinal(this.lastPushed);
    }

    forkCleanup(timestamp: string, lastNonForked: number, lastForked: number): void {
        this.purgeMemoryFrom(lastNonForked + 1);
        this.lastPushed = lastNonForked;
        // this.lastBlock = this._rowFromCurrentSeries(lastNonForked);
    }

    blockScroll(params: {
        from: number;
        to: number;
        tag: string;
        logLevel?: string;
        validate?: boolean;
        scrollOpts?: any
    }): BlockScroller {
        return null;
    }
}