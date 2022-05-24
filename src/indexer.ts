import StateHistoryBlockReader from './ship';
import {
    ShipBlock,
    ShipBlockResponse,
    ShipTableDelta,
    ShipAccountDelta,
    ShipTransactionTrace
} from './types/ship';

import {
    EosioAction,
    AbiDocument
} from './types/eosio';

import {
    extractShipContractRows,
    extractShipTraces,
    deserializeEosioType,
    getTableAbiType,
    getActionAbiType
} from './utils/eosio';

import * as eosioEvmAbi from './abis/evm.json';
import * as eosioTokenAbi from './abis/token.json'
import * as eosioSystemAbi from './abis/system.json'

import logger from './utils/winston';

import { Serialize , RpcInterfaces } from 'eosjs';

import { handleEvmTx, handleEvmDeposit, handleEvmWithdraw } from './handlers';

import { ElasticConnector } from './database';

import * as AbiEOS from "@eosrio/node-abieos";

import { IndexerStateDocument } from './types/indexer';

const createHash = require("sha1-uint8array").createHash

const encoder = new TextEncoder;
const decoder = new TextDecoder;
const abiTypes = Serialize.getTypesFromAbi(Serialize.createAbiTypes());

function createSerialBuffer(inputArray: Uint8Array) {
    return new Serialize.SerialBuffer({
        textEncoder: encoder,
        textDecoder: decoder,
        array: inputArray
    });
}

function getContract(contractAbi: RpcInterfaces.Abi) {
    const types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), contractAbi)
    const actions = new Map()
    for (const { name, type } of contractAbi.actions) {
        actions.set(name, Serialize.getType(types, type))
    }
    return { types, actions }
}

function deserialize(types: Map<string, Serialize.Type>, array: Uint8Array) {
    const buffer = new Serialize.SerialBuffer(
        { textEncoder: encoder, textDecoder: decoder, array });

    let result = Serialize.getType(types, "transaction")
        .deserialize(buffer, new Serialize.SerializerState({ bytesAsUint8Array: true }));

    return result;
}


function getErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message
  return String(error)
}


function hashTxAction(action: EosioAction) {
    // // debug mode, pretty responses
    // let uid = action.account;
    // uid = uid + "." + action.name;
    // for (const auth of action.authorization) {
    //     uid = uid + "." + auth.actor;
    //     uid = uid + "." + auth.permission;
    // }
    // uid = uid + "." + createHash().update(action.data).digest("hex");
    // return uid;

    // release mode, only hash
    const hash = createHash();
    hash.update(action.account);
    hash.update(action.name);
    for (const auth of action.authorization) {
        hash.update(auth.actor);
        hash.update(auth.permission);
    }
    hash.update(action.data);
    return hash.digest("hex");
}


export class TEVMIndexer {

    endpoint: string;
    contracts: {[key: string]: Serialize.Contract};
    abis: {[key: string]: RpcInterfaces.Abi};

    currentBlock: number;
    startBlock: number;
    stopBlock: number;

    reader: StateHistoryBlockReader;
    connector: ElasticConnector;

    constructor(
        endpoint: string,
        startBlock: number,
        stopBlock: number
    ) {

        this.endpoint = endpoint;
        this.startBlock = startBlock;
        this.stopBlock = stopBlock;

        this.connector = new ElasticConnector();

        this.reader = new StateHistoryBlockReader(endpoint);
        this.reader.setOptions({
            min_block_confirmation: 1,
            ds_threads: 8,
            allow_empty_deltas: false,
            allow_empty_traces: false,
            allow_empty_blocks: false
        });

        this.abis = {
            'eosio.evm': eosioEvmAbi.abi,
            'eosio.token': eosioTokenAbi.abi,
            'eosio': eosioSystemAbi.abi
        };
        this.contracts = {
            'eosio.evm': getContract(eosioEvmAbi.abi),
            'eosio.token': getContract(eosioTokenAbi.abi),
            'eosio': getContract(eosioSystemAbi.abi)
        };
    }

    async consumer(resp: ShipBlockResponse): Promise<void> {

        this.currentBlock = resp.this_block.block_num;
        if (this.currentBlock % 1000 == 0)
            logger.info(this.currentBlock + " indexed, ")

        let signatures: {[key: string]: string[]} = {};

        for (const tx of resp.block.transactions) {

            if (tx.trx[0] !== "packed_transaction")
                continue;

            const packed_trx = tx.trx[1].packed_trx;

            try {
                const trx = deserialize(
                    this.reader.types, packed_trx);

                for (const action of trx.actions) {
                    signatures[hashTxAction(action)] = tx.trx[1].signatures;
                }

            } catch (error) {
                logger.error(getErrorMessage(error) + ": " + tx);
            }
        }

        // process deltas to catch evm block num
        let eosioGlobalState = null;
        const contractDeltas = extractShipContractRows(resp.deltas);
        for (const delta of contractDeltas) {
            if (delta.code == "eosio" &&
                delta.scope == "eosio" &&
                delta.table == "global") {

                const type = getTableAbiType(eosioSystemAbi.abi, delta.code, delta.table);
                eosioGlobalState = deserializeEosioType(
                    type,
                    delta.value,
                    this.contracts[delta.code].types);
            }
        }

        if (eosioGlobalState == null)
            throw new Error("Couldn't get eosio global state table delta.");

        const evmBlockNumber = eosioGlobalState.block_num;
        const evmTransactions = [];
        // traces
        const transactions = extractShipTraces(resp.traces);

        for (const tx of transactions) {
            const contractWhitelist = ["eosio.evm", "eosio.token"];
            const actionWhitelist = ["raw", "withdraw", "transfer"]

            const action = tx.trace.act;

            if (!contractWhitelist.includes(action.account) ||
                !actionWhitelist.includes(action.name))
                continue;

            const type = getActionAbiType(
                this.abis[action.account],
                action.account, action.name);

            const actionData = deserializeEosioType(
                type, action.data, this.contracts[action.account].types);

            if (action.name == "transfer" && actionData.to != "eosio.evm")
                continue;

            const actionHash = hashTxAction(action);
            if (!(actionHash in signatures))
                throw new Error("Could't find signature that matches trace.");

            const signature = signatures[actionHash][0];

            let evmTx = null;
            if (action.account == "eosio.evm") {
                if (action.name == "raw") {
                    evmTx = await handleEvmTx(actionData, signature);
                } else if (action.name == "withdraw" ){
                    evmTx = await handleEvmWithdraw(actionData, signature);
                }
            } else if (action.account == "eosio.token" &&
                    action.name == "transfer" &&
                    actionData.to == "eosio.evm") {
                evmTx = await handleEvmDeposit(actionData, signature);
            } else
                continue;

            evmTransactions.push(evmTx);
            
        }

    }

    async launch() {

        let prevState = null;

        let startBlock = this.startBlock;
        let stopBlock = this.stopBlock;
            
        try {
            prevState = await this.connector.getIndexerState();
            logger.info(prevState);

            startBlock = prevState.lastIndexedBlock;
        } catch (error) {
            logger.warn(error);
        }

        this.reader.consume(this.consumer.bind(this));

        this.reader.startProcessing({
            start_block_num: startBlock,
            end_block_num: stopBlock,
            max_messages_in_flight: 10,
            irreversible_only: true,
            have_positions: [],
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true
        }, ['contract_row', 'contract_table']);

        process.on('SIGINT', this.sigintHandler);
    }

    sigintHandler() {
        logger.info("interrupt caught, saving state to db...");

        const state = {
            timestamp: new Date().toISOString(),
            lastIndexedBlock: this.currentBlock
        };

        this.connector.indexState(state).then(() => {
            process.exit(0);
        }); 
    }

};
