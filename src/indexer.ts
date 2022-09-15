import StateHistoryBlockReader from './ship';
import {
    ShipBlockResponse
} from './types/ship';

import { StaticPool } from 'node-worker-threads-pool';

import { IndexerConfig, IndexerState } from './types/indexer';

import {
    extractGlobalContractRow,
    extractShipTraces,
    deserializeEosioType,
    getTableAbiType,
    getActionAbiType,
    getRPCClient
} from './utils/eosio';

import * as eosioEvmAbi from './abis/evm.json'
import * as eosioMsigAbi from './abis/msig.json';
import * as eosioTokenAbi from './abis/token.json'
import * as eosioSystemAbi from './abis/system.json'

import logger from './utils/winston';

import { Serialize , RpcInterfaces, JsonRpc } from 'eosjs';

import { 
    setCommon,
    handleEvmTx, handleEvmDeposit, handleEvmWithdraw, TxDeserializationError, isTxDeserializationError
} from './handlers';

import {StorageEvmTransaction} from './types/evm';

import { hashTxAction } from './ship';
import {Connector} from './database/connector';

import { BlockHeader } from './utils/evm'

import moment from 'moment';

const BN = require('bn.js');

const sleep = (ms: number) => new Promise( res => setTimeout(res, ms));

interface InprogressBuffers {
    evmTransactions: Array<{
        trx_id: string,
        action_ordinal: number,
        signatures: string[],
        evmTx: StorageEvmTransaction
    }>;
    errors: TxDeserializationError[];
    evmBlockNum: number;
};


function getContract(contractAbi: RpcInterfaces.Abi) {
    const types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), contractAbi)
    const actions = new Map()
    for (const { name, type } of contractAbi.actions) {
        actions.set(name, Serialize.getType(types, type))
    }
    return { types, actions }
}

const deltaType = getTableAbiType(eosioSystemAbi.abi, 'eosio', 'global');

export class TEVMIndexer {
    endpoint: string;
    wsEndpoint: string;

    evmDeployBlock: number;
    startBlock: number;
    stopBlock: number;

    ethGenesisHash: string;

    inprogress: Map<number, InprogressBuffers> = new Map();

    knownBlocks: number[] = [];

    state: IndexerState = IndexerState.SYNC;
    switchingState: boolean = false;
    cleanupInProgress: boolean = false;

    lastEvmBlockNum: number;

    txsSinceLastReport: number = 0;

    config: IndexerConfig;

    contracts: {[key: string]: Serialize.Contract};
    abis: {[key: string]: RpcInterfaces.Abi};

    hasher: StaticPool<(x: {type: string, params: any}) => any>;
    reader: StateHistoryBlockReader;
    rpc: JsonRpc;
    connector: Connector; 
    
    constructor(telosConfig: IndexerConfig) {
        this.config = telosConfig;

        this.endpoint = telosConfig.endpoint;
        this.wsEndpoint = telosConfig.wsEndpoint;

        this.evmDeployBlock = telosConfig.evmDeployBlock;

        this.startBlock = telosConfig.startBlock;
        this.stopBlock = telosConfig.stopBlock;

        this.rpc = getRPCClient(telosConfig);
        this.connector = new Connector(telosConfig, false); 

        this.reader = new StateHistoryBlockReader(this.wsEndpoint);
        this.reader.setOptions({
            min_block_confirmation: 0,
            ds_threads: telosConfig.perf.workerAmount,
            allow_empty_deltas: true,
            allow_empty_traces: true,
            allow_empty_blocks: true
        });

        this.abis = {
            'eosio.evm': eosioEvmAbi.abi,
            'eosio.msig': eosioMsigAbi.abi,
            'eosio.token': eosioTokenAbi.abi,
            'eosio': eosioSystemAbi.abi
        };
        this.contracts = {
            'eosio.evm': getContract(eosioEvmAbi.abi),
            'eosio.msig': getContract(eosioMsigAbi.abi),
            'eosio.token': getContract(eosioTokenAbi.abi),
            'eosio': getContract(eosioSystemAbi.abi)
        };
        
        setCommon(telosConfig.chainId);
    }

    async consumer(resp: ShipBlockResponse): Promise<void> {

        // SYNC & HEAD mode swtich detection
        const blocksUntilHead = resp.head.block_num - resp.this_block.block_num;

        if (this.state == IndexerState.SYNC &&
            !this.switchingState &&
            blocksUntilHead <= this.config.perf.elasticDumpSize) {
            this.switchingState = true;
            await this.hasher.exec({
                type: 'state', params: {
                    state: IndexerState.HEAD
                }});
            this.state = IndexerState.HEAD;

            logger.info(
                'switched to HEAD mode! blocks will be written to db asap.');
            this.switchingState = false;
        }

        // fork detection
        let forkData = null;
        if (!this.cleanupInProgress) {
            if (resp.this_block.block_num in this.knownBlocks) {
                const newHead = resp.this_block.block_num;
                const msg = `fork detected!, reverse all blocks after ${newHead}`;
                logger.warn(msg);

                forkData = new TxDeserializationError(
                    msg, {
                        'known': this.knownBlocks,
                        'newHead': newHead
                    });
            
                this.cleanupInProgress = true;
                // await hasher worker and db cleanup
                await this.hasher.exec({
                    type: 'fork', params: {
                        blockNum: newHead 
                    }});

                // delete forked knownBlocks
                while (this.knownBlocks.pop() != newHead)
                    continue

                this.cleanupInProgress = false;
            }
        } else {
            while (this.cleanupInProgress)
                await new Promise(f => setTimeout(f, 200));
        }

        const currentBlock = resp.this_block.block_num;
        this.knownBlocks.push(currentBlock);

        // only allow up to half an hour of blocks 
        if (this.knownBlocks.length > 3600)
            this.knownBlocks.splice(3599)

        // process deltas to catch evm block num
        const globalDelta = extractGlobalContractRow(resp.deltas);

        let buffs: InprogressBuffers = null;

        if (globalDelta != null) {
            const eosioGlobalState = deserializeEosioType(
                deltaType, globalDelta.value, this.contracts['eosio'].types);

            this.lastEvmBlockNum = eosioGlobalState.block_num;
            if (this.inprogress.has(this.lastEvmBlockNum))
                buffs = this.inprogress.get(this.lastEvmBlockNum);
            else {
                buffs = {
                    evmTransactions: [],
                    errors: [],
                    evmBlockNum: this.lastEvmBlockNum
                };
                this.inprogress.set(
                    this.lastEvmBlockNum, buffs);
            }
        } else {
            buffs = {
                evmTransactions: [],
                errors: [],
                evmBlockNum: this.lastEvmBlockNum + 1
            };
            this.inprogress.set(
                this.lastEvmBlockNum + 1, buffs);
        }

        const evmBlockNum = buffs.evmBlockNum;
        const evmTransactions = buffs.evmTransactions;
        const errors = buffs.errors;

        // inject fork data into errors
        if (forkData != null)
            errors.push(forkData);

        // traces
        const transactions = extractShipTraces(resp.traces);
        let gasUsedBlock = 0;
        const systemAccounts = [ 'eosio', 'eosio.stake', 'eosio.ram' ];
        const contractWhitelist = [
            "eosio.evm", "eosio.token",  // evm
            "eosio.msig"  // deferred transaction sig catch
        ];
        const actionWhitelist = [
            "raw", "withdraw", "transfer",  // evm
            "exec" // msig deferred sig catch 
        ]

        for (const tx of transactions) {

            const action = tx.trace.act;

            if (!contractWhitelist.includes(action.account) ||
                !actionWhitelist.includes(action.name))
                continue;

            const type = getActionAbiType(
                this.abis[action.account],
                action.account, action.name);

            const actionData = deserializeEosioType(
                type, action.data, this.contracts[action.account].types);

            // discard transfers to accounts other than eosio.evm
            // and transfers from system accounts
            if ((action.name == "transfer" && actionData.to != "eosio.evm") || 
               (action.name == "transfer" && actionData.from in systemAccounts))
                continue;

            // find correct auth in related traces list
            let foundSig = false;
            let actionHash = "";
            for (const trace of tx.tx.traces) {
                actionHash = hashTxAction(trace.act);
                if (actionHash in resp.block.signatures) {
                    foundSig = true;
                    break;
                }
            }

            let evmTx: StorageEvmTransaction | TxDeserializationError = null;
            if (action.account == "eosio.evm") {
                if (action.name == "raw") {
                    evmTx = await handleEvmTx(
                        resp.this_block.block_id,
                        evmTransactions.length,
                        evmBlockNum,
                        actionData,
                        tx.trace.console
                    );
                    if (!isTxDeserializationError(evmTx))
                        gasUsedBlock = evmTx.gasusedblock;
                } else if (action.name == "withdraw"){
                    evmTx = await handleEvmWithdraw(
                        resp.this_block.block_id,
                        evmTransactions.length,
                        evmBlockNum,
                        actionData,
                        this.rpc,
                        gasUsedBlock
                    );
                }
            } else if (action.account == "eosio.token" &&
                    action.name == "transfer" &&
                    actionData.to == "eosio.evm") {
                evmTx = await handleEvmDeposit(
                    resp.this_block.block_id,
                    evmTransactions.length,
                    evmBlockNum,
                    actionData,
                    this.rpc,
                    gasUsedBlock
                );
            } else
                continue;

            if (isTxDeserializationError(evmTx)) {
                if (this.config.debug) {
                    errors.push(evmTx);
                    continue;
                } else
                    throw new Error(JSON.stringify(evmTx));
            }

            let signatures: string[] = [];
            if (foundSig)
                signatures = resp.block.signatures[actionHash];

            evmTransactions.push({
                trx_id: tx.tx.id,
                action_ordinal: tx.trace.action_ordinal,
                signatures: signatures,
                evmTx: evmTx
            });
            this.txsSinceLastReport++;
        }

        if (globalDelta != null) {
            const hasherResp = await this.hasher.exec({
                type: 'block', params: {
                    nativeBlockHash: resp.block.block_id,
                    nativeBlockNumber: currentBlock,
                    evmBlockNumber: evmBlockNum,
                    blockTimestamp: resp.block.timestamp,
                    evmTxs: evmTransactions,
                    errors: errors
                }});

            if (!hasherResp.success) {
                logger.error(hasherResp);
                process.exit(1);
            }

            if (currentBlock % 1000 == 0) {
                logger.info(`${currentBlock} indexed, ${this.txsSinceLastReport} txs.`)
                this.txsSinceLastReport = 0;
            }

            this.inprogress.delete(evmBlockNum);
        }
        
    }

    async launch() {

        let genesisBlock = null;
        while(genesisBlock == null) {
            try {
                // get genesis information
                genesisBlock = await this.rpc.get_block(
                    this.evmDeployBlock - 1);

            } catch (e) {
                logger.warn('couldn\'t get genesis block retrying in 5 seg...');
                await sleep(5000);
                continue
            }
        }

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

        let startBlock = this.startBlock;
        let stopBlock = this.stopBlock;
        let prevHash = null;

        await this.connector.init();

        logger.info('checking db for blocks...');
        const lastBlock = await this.connector.getLastIndexedBlock();

        if (lastBlock != null) {
            // found blocks on the database
            logger.info(JSON.stringify(lastBlock, null, 4));
            startBlock = lastBlock.block_num;
            prevHash = lastBlock['@evmBlockHash'];
            logger.info(
                `found! ${startBlock} produced on ${lastBlock['@timestamp']} with hash 0x${prevHash}`);

        } else {
            // prev blocks not found, start from genesis or EVM_PREV_HASH
            if (this.config.startBlock == this.config.evmDeployBlock) {
                prevHash = this.ethGenesisHash;

            } else if (this.config.evmPrevHash != '') {
                prevHash = this.config.evmPrevHash;

            } else
                throw new Error('Configuration error, no way to get prev hash.');

            logger.info(`start from ${startBlock} with hash 0x${prevHash}.`);
        }

        this.hasher = new StaticPool({
            size: 1,
            task: './build/workers/hasher.js',
            workerData: {
                config: this.config,
                startBlock: startBlock,
                prevHash: prevHash 
            }
        });

        this.reader.consume(this.consumer.bind(this));

        this.reader.startProcessing({
            start_block_num: startBlock,
            end_block_num: stopBlock,
            max_messages_in_flight: this.config.perf.maxMsgsInFlight,
            irreversible_only: false,
            have_positions: [],
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true
        }, ['contract_row', 'contract_table']);

    }

};
