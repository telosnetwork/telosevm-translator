import {
    IndexedAccountDelta,
    IndexedAccountStateDelta, IndexedBlock, IndexedBlockHeader, IndexedInternalTx, IndexedTx, IndexedTxLog
} from "../../types/indexer.js";
import {BlockScroller, Connector} from "../connector.js";
import {BLOCK_GAS_LIMIT} from "../../utils/evm.js";

import {Name} from "@wharfkit/antelope";
import cloneDeep from "lodash.clonedeep";
import {ArrowConnectorConfig, ConnectorConfig} from "../../types/config.js";
import {
    ArrowBatchContextDef,
    ArrowBatchTableDef,
    ArrowBatchWriter,
    RowWithRefs
} from "@guilledk/arrowbatch-nodejs";
import {featureManager} from "../../features.js";


export const translatorDataContext: ArrowBatchContextDef = {
    root: {
        name: 'block',
        ordinal: 'block_num',
        map: [
            {name: 'timestamp',           type: 'u64'},
            {name: 'block_hash',          type: 'checksum256'},
            {name: 'evm_block_hash',      type: 'checksum256'},
            {name: 'evm_prev_block_hash', type: 'checksum256'},
            {name: 'receipts_hash',       type: 'checksum256'},
            {name: 'txs_hash',            type: 'checksum256'},
            {name: 'gas_used',            type: 'uintvar'},
            {name: 'txs_amount',          type: 'u32'},
            {name: 'size',                type: 'u32'},
        ]
    },
    others: {
        // transactions
        tx: {
            map: [
                {name: 'id', type: 'checksum256'},
                {name: 'global_index', type: 'u64'},
                {name: 'block_num', type: 'u64', ref: {table: 'root', field: 'block_num'}},
                {name: 'action_ordinal', type: 'u32'},

                {name: 'raw', type: 'bytes'},
                {name: 'hash', type: 'checksum256'},
                {name: 'from', type: 'checksum160', optional: true},
                {name: 'evm_ordinal', type: 'u32'},
                {name: 'block_hash', type: 'checksum256'},
                {name: 'to', type: 'bytes', optional: true},
                {name: 'input', type: 'bytes'},
                {name: 'value', type: 'uintvar'},
                {name: 'nonce', type: 'uintvar'},
                {name: 'gas_price', type: 'uintvar'},
                {name: 'gas_limit', type: 'uintvar'},
                {name: 'status', type: 'u8'},
                {name: 'itx_amount', type: 'u32'},
                {name: 'epoch', type: 'u32'},
                {name: 'created_addr', type: 'checksum160', optional: true},
                {name: 'gas_used', type: 'uintvar'},
                {name: 'gas_used_block', type: 'uintvar'},
                {name: 'charged_price', type: 'uintvar'},
                {name: 'output', type: 'bytes'},
                {name: 'logs_amount', type: 'u32'},
                {name: 'logs_bloom', type: 'bytes', optional: true},
                {name: 'errors', type: 'string', optional: true, array: true},
                {name: 'v', type: 'uintvar'},
                {name: 'r', type: 'uintvar'},
                {name: 's', type: 'uintvar'}
            ],
            streamSize: "256MB"
        },
        tx_log: {
            map: [
                {name: 'tx_index', type: 'u64', ref: {table: 'tx', field: 'global_index'}},
                {name: 'log_index', type: 'u32'},
                {name: 'address', type: 'checksum160', optional: true},
                {name: 'data', type: 'bytes', optional: true},
                {name: 'topics', type: 'bytes', array: true, optional: true},
            ]
        },

        // telos.evm state deltas
        account: {
            map: [
                {name: 'timestamp', type: 'u64'},
                {name: 'block_num', type: 'u64', ref: {table: 'root', field: 'block_num'}},
                {name: 'block_index', type: 'u32'},
                {name: 'index', type: 'u64'},
                {name: 'address', type: 'checksum160'},
                {name: 'account', type: 'u64'},
                {name: 'nonce', type: 'u64'},
                {name: 'code', type: 'bytes'},
                {name: 'balance', type: 'bytes', length: 32},
            ]
        },
        accountstate: {
            map: [
                {name: 'timestamp', type: 'u64'},
                {name: 'block_num', type: 'u64', ref: {table: 'root', field: 'block_num'}},
                {name: 'block_index', type: 'u32'},
                {name: 'scope', type: 'u64'},
                {name: 'index', type: 'u64'},
                {name: 'key', type: 'bytes', length: 32},
                {name: 'value', type: 'bytes', length: 32}
            ]
        }
    }
};

// 1.x compat, internal transactions
const itxDef: ArrowBatchTableDef = {
    map: [
        {name: 'tx_index', type: 'u64', ref: {table: 'tx', field: 'global_index'}},
        {name: 'itx_index', type: 'u32'},
        {name: 'call_type', type: 'bytes'},
        {name: 'from', type: 'checksum160'},
        {name: 'gas', type: 'uintvar'},
        {name: 'input', type: 'bytes'},
        {name: 'to', type: 'bytes'},
        {name: 'value', type: 'uintvar'},
        {name: 'gas_used', type: 'uintvar'},
        {name: 'output', type: 'bytes'},
        {name: 'subtraces', type: 'u16'},
        {name: 'type', type: 'string'},
        {name: 'depth', type: 'string'}
    ]
};


export class ArrowBlockScroller extends BlockScroller {

    private lastBlock: bigint;

    constructor(
        connector: ArrowConnector,
        params: {
            from: bigint,
            to: bigint,
            tag: string
            logLevel?: string,
            validate?: boolean
        }
    ) {
        super(connector, params);
        this.lastBlock = params.from - 1n;
    }

    async init(): Promise<void> {}

    async nextResult(): Promise<IndexedBlock> {
       const nextBlock = await this.connector.getIndexedBlock(this.lastBlock + 1n);
       this.lastBlock++;
       return nextBlock;
    }
}

export class ArrowConnector extends Connector {

    readonly pconfig: ArrowConnectorConfig;

    private writer: ArrowBatchWriter;

    private globalTxIndex: bigint = BigInt(0);

    constructor(config: ConnectorConfig) {
        super(config);

        if (!config.arrow)
            throw new Error(`Tried to init polars connector with null config`);

        this.pconfig = config.arrow;

        const dataContext = cloneDeep(translatorDataContext);
        if (featureManager.isFeatureEnabled('STORE_ITXS'))
            dataContext.others.itx = itxDef;

        this.writer = new ArrowBatchWriter(
            config.arrow,
            dataContext,
            this.logger
        );
    }

    private rowFromItx(
        ordinal: number,
        itx: IndexedInternalTx,
        ref: any
    ) {
          return {
              row: [
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
                  itx.subTraces,
                  itx.type,
                  itx.depth
                ],
            refs: new Map()
        };
    }

    itxFromRow(fullRow: RowWithRefs): IndexedInternalTx {
        const row = fullRow.row;
        return {
            callType: row[2],
            from: row[3],
            gas: row[4],
            input: row[5],
            to: row[6],
            value: row[7],
            gasUsed: row[8],
            output: row[9],
            subTraces: row[10],
            traceAddress: row[11],
            type: row[12],
            depth: row[13]
        }
    }

    private rowFromTxLog(
        ordinal: number,
        log: IndexedTxLog,
        ref: any
    ) {
        return {
            row: [
                this.globalTxIndex,
                ordinal,
                log.address,
                log.data,
                log.topics
            ],
            refs: new Map()
        };
    }

    txLogFromRow(fullRow: RowWithRefs): IndexedTxLog {
        const row = fullRow.row;
        return {
            address: row[2],
            topics: row[3],
            data: row[4]
        }
    }

    private rowFromTx(
        tx: IndexedTx
    ) {
        const refs = new Map<string, RowWithRefs[]>();
        const row = [
            tx.trxId,
            this.globalTxIndex,
            tx.blockNum + this.config.chain.evmBlockDelta,
            tx.actionOrdinal,

            tx.raw,
            tx.hash,
            tx.from,
            tx.trxIndex,
            tx.blockHash,
            tx.to,
            tx.inputData,
            tx.value,
            tx.nonce,
            tx.gasPrice,
            tx.gasLimit,
            tx.status,
            tx.itxs.length,
            tx.epoch,
            tx.createAddr,
            tx.gasUsed,
            tx.gasUsedBlock,
            tx.chargedGasPrice,
            tx.output,
            tx.logs.length,
            tx.logsBloom,
            tx.errors,
            tx.v, tx.r, tx.s
        ];

        if (featureManager.isFeatureEnabled('STORE_ITXS')) {
            refs.set(
                'itx',
                tx.itxs.map(
                    (itx, index) => this.rowFromItx(index, itx, tx))
            );
        }

        refs.set(
            'itx',
            tx.logs.map(
                (log, index) => this.rowFromTxLog(index, log, tx))
        );

        this.globalTxIndex++;

        return {
            row,
            refs
        }
    }

    txFromRow(fullRow: RowWithRefs): IndexedTx {
        const row = fullRow.row;

        let itxs = undefined;
        if (featureManager.isFeatureEnabled('STORE_ITXS')) {
            const refs = fullRow.refs.get('itx') ?? [];
            itxs = refs.map(itxRow => this.itxFromRow(itxRow));
        }

        const refs = fullRow.refs.get('tx_log') ?? [];
        const logs = refs.map(logRow => this.txLogFromRow(logRow));

        return {
            trxId: row[0],
            trxIndex: row[7],
            actionOrdinal: row[3],
            blockNum: row[2],
            blockHash: row[8],

            hash: row[5],
            raw: row[4],

            from: row[6],
            to: row[9],
            inputData: row[10],
            value:  row[11],
            nonce:  row[12],
            gasPrice:  row[13],
            gasLimit:  row[14],
            v:  row[26],
            r:  row[27],
            s:  row[28],

            // receipt
            status: row[15],
            itxs,
            epoch: row[17],
            createAddr: row[18],
            gasUsed: row[19],
            gasUsedBlock: row[20],
            chargedGasPrice: row[21],
            output:  row[22],
            logs,
            logsBloom: row[24],
            errors: row[25]
        }
    }

    private rowFromAccount(delta: IndexedAccountDelta) {
        return {
            row: [
                delta.timestamp,
                delta.blockNum,
                delta.ordinal,
                delta.index,
                delta.address,
                Name.from(delta.account).value.toString(),
                delta.nonce,
                delta.code,
                delta.balance
            ],
            refs: new Map()
        };
    }

    private rowFromAccountState(delta: IndexedAccountStateDelta) {
        return {
            row: [
                delta.timestamp,
                delta.blockNum,
                delta.ordinal,
                Name.from(delta.scope).value.toString(),
                delta.index,
                delta.key,
                delta.value
            ],
            refs: new Map()
        };
    }

    accountDeltaFromRow(fullRow: RowWithRefs): IndexedAccountDelta {
        const row = fullRow.row;
        return {
            timestamp: row[0],
            blockNum: row[1],
            ordinal: row[2],
            index: row[3],
            address: row[4],
            account: row[5],
            nonce: row[6],
            code: row[7],
            balance: row[8]
        }
    }

    accountStateDeltaFromRow(fullRow: RowWithRefs): IndexedAccountStateDelta {
        const row = fullRow.row;
        return {
            timestamp: row[0],
            blockNum: row[1],
            ordinal: row[2],
            scope: row[3],
            index: row[4],
            key: row[5],
            value: row[6]
        }
    }

    blockFromRow(fullRow: RowWithRefs): IndexedBlock {
        const row = fullRow.row;

        const txRefs = fullRow.refs.get('tx') ?? [];
        const transactions = txRefs.map(txRow => this.txFromRow(txRow));

        let account = [];
        let accountstate = [];

        if (featureManager.isFeatureEnabled('STORE_ACC_DELTAS')) {
            const accDeltaRefs = fullRow.refs.get('account') ?? [];
            account = accDeltaRefs.map(accDeltaRow => this.accountDeltaFromRow(accDeltaRow));

            const accStateDeltaRefs = fullRow.refs.get('accountstate') ?? [];
            accountstate = accStateDeltaRefs.map(
                accStateDeltaRow => this.accountStateDeltaFromRow(accStateDeltaRow));
        }

        return {
            timestamp: row[1],

            blockNum: row[0],
            blockHash: row[2],

            evmBlockNum: row[0] - this.config.chain.evmBlockDelta,
            evmBlockHash: row[3],
            evmPrevHash: row[4],

            receiptsRoot: row[5],
            transactionsRoot: row[6],
            gasUsed: row[7],
            gasLimit: BLOCK_GAS_LIMIT,
            transactionAmount: row[8],
            size: row[9],

            transactions,
            logsBloom: new Uint8Array(),
            deltas: {
                account,
                accountstate
            }
        }
    }

    private rowFromBlock(block: IndexedBlock) {
        const blockRow = {
            row: [
                block.blockNum,
                block.timestamp,
                block.blockHash,
                block.evmBlockHash,
                block.evmPrevHash,
                block.receiptsRoot,
                block.transactionsRoot,
                block.gasUsed,
                block.transactionAmount,
                Number(block.size)
            ],
            refs: new Map()
        };

        blockRow.refs.set(
            'tx',
            block.transactions.map(
                tx => this.rowFromTx(tx))
        );

        if (featureManager.isFeatureEnabled('STORE_ACC_DELTAS')) {
            blockRow.refs.set(
                'account',
                block.deltas.account.map(
                    accountDelta => this.rowFromAccount(accountDelta))
            );

            blockRow.refs.set(
                'accountstate',
                block.deltas.accountstate.map(
                    stateDelta => this.rowFromAccountState(stateDelta))
            );
        }

        return blockRow;
    }

    async init(): Promise<bigint | null> {
        await this.writer.init(
            this.config.chain.startBlock);
        return null;
    }

    async deinit(): Promise<void> {
        await this.flush();

        await this.writer.deinit();

        if (this.isBroadcasting)
            this.stopBroadcast();
    }

    async getAccountDeltasForBlock(blockNum: bigint): Promise<IndexedAccountDelta[]> {
        const block = await this.getIndexedBlock(blockNum);
        return block.deltas.account;
    }

    async getAccountStateDeltasForBlock(blockNum: bigint): Promise<IndexedAccountStateDelta[]> {
        const block = await this.getIndexedBlock(blockNum);
        return block.deltas.accountstate;
    }

    async getBlockHeader(blockNum: bigint): Promise<IndexedBlockHeader | null> {
        const block = await this.getIndexedBlock(blockNum);
        delete block.deltas;
        delete block.transactions;
        return block;
    }

    async getTransactionsForBlock(blockNum: bigint): Promise<IndexedTx[]> {
        const block = await this.getIndexedBlock(blockNum);
        return block.transactions;
    }

    async getFirstIndexedBlock(): Promise<IndexedBlock | null> {
        if (!this.writer.firstOrdinal)
            return null;

        const rootRow = await this.writer.getRow(this.writer.firstOrdinal);
        return this.blockFromRow(rootRow);
    }

    async getIndexedBlock(blockNum: bigint): Promise<IndexedBlock | null> {
        const rootRow = await this.writer.getRow(blockNum);
        return this.blockFromRow(rootRow);
    }

    async getLastIndexedBlock(): Promise<IndexedBlock> {
        if (!this.writer.lastOrdinal)
            return null;

        const rootRow = await this.writer.getRow(this.writer.lastOrdinal);
        return this.blockFromRow(rootRow);
    }

    async purgeNewerThan(blockNum: bigint): Promise<void> {
        await this.writer.trimFrom(blockNum);
    }

    async fullIntegrityCheck(): Promise<bigint | null> { return null; }

    async flush(): Promise<void> {
        await new Promise<void>(resolve => {
            this.writer.events.on('flush', () => {
                resolve();
            });
            this.writer.beginFlush();
        });
    }

    async pushBlock(block: IndexedBlock): Promise<void> {
        if (this.lastPushedHash && block.evmPrevHash !== this.lastPushedHash) {
            throw new Error(`Attempted to build invalid chain, lastpushed hash: ${this.lastPushedHash}, current block prev hash: ${block.evmPrevHash}`);
        }
        const rootRow: RowWithRefs = this.rowFromBlock(block);
        this.writer.pushRow('block', rootRow);
        this.lastPushed = block.blockNum;
        this.lastPushedHash = block.evmBlockHash;
    }

    forkCleanup(timestamp: bigint, lastNonForked: bigint, lastForked: bigint): void {}

    blockScroll(params: {
        from: bigint;
        to: bigint;
        tag: string;
        logLevel?: string;
        validate?: boolean;
        scrollOpts?: any
    }): BlockScroller {
        return new ArrowBlockScroller(this, params);
    }
}