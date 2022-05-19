import StateHistoryBlockReader from './ship';
import {
    ShipBlock,
    ShipBlockResponse,
    ShipTableDelta,
    ShipAccountDelta,
    ShipTransactionTrace
} from './types/ship';

import {
    AbiDocument
} from './types/eosio';

import * as eosioEvmAbi from './abis/evm.json';
import * as eosioTokenAbi from './abis/token.json'
import * as eosioSystemAbi from './abis/system.json'

import logger from './utils/winston';

import { Serialize , RpcInterfaces } from 'eosjs';

import { handleEvmTx, handleEvmDeposit, handleEvmWithdraw } from './handlers';

import { ElasticConnector } from './database';

import * as AbiEOS from "@eosrio/node-abieos";

import { IndexerStateDocument } from './types/indexer';


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


export class TEVMIndexer {

    endpoint: string;
    abiWhitelist: string[];

    defaultAbis: {[key: string]: Serialize.Contract};
    abis: {[key: string]: Serialize.Contract};

    currentBlock: number;
    startBlock: number;
    stopBlock: number;

    reader: StateHistoryBlockReader;
    connector: ElasticConnector;

    constructor(
        endpoint: string,
        abiWhitelist: string[],
        startBlock: number,
        stopBlock: number
    ) {

        this.endpoint = endpoint;
        this.abiWhitelist = abiWhitelist;
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

        this.defaultAbis = {
            'eosio.evm': getContract(eosioEvmAbi.abi),
            'eosio.token': getContract(eosioTokenAbi.abi),
            'eosio': getContract(eosioSystemAbi.abi)
        };
    }

    getContract(account: string) {
        if (account in this.abis)
            return this.abis[account];

        if (account in this.defaultAbis)
            return this.defaultAbis[account];

        return null;
    }

    async consumer(resp: ShipBlockResponse): Promise<void> {

        this.currentBlock = resp.this_block.block_num;
        if (this.currentBlock % 1000 == 0)
            logger.info(this.currentBlock + " indexed, ")

        // process deltas to catch abi updates
        for (const delta of resp.deltas) {

            if (delta[0] != "table_delta_v0")
                continue;

            const name = delta[1].name;
            const rows = delta[1].rows;

            if (name == "account") {
                for (const row of rows) {
                    if (row.present) {
                        // @ts-ignore
                        const data: Uint8Array = row.data;
                        const [type, accountObj] = AbiEOS.bin_to_json(
                            "0", "account", Buffer.from(data))

                        // @ts-ignore
                        const accountDelta: ShipAccountDelta = accountObj;

                        if (!(accountDelta.account in this.abiWhitelist))
                            continue;

                        const abiHex: string = accountDelta.abi;
                        const abiBin = new Uint8Array(Buffer.from(abiHex, 'hex'));
                        const initialTypes = Serialize.createInitialTypes();
                        const abiObj = abiTypes.get('abi_def').deserialize(createSerialBuffer(abiBin));

                        this.abis[accountDelta.account] = getContract(abiObj);

                        const abiDocument = {
                            block_num: this.currentBlock,
                            timestamp: accountDelta.creation_date,
                            account: accountDelta.account,
                            abi: abiObj
                        };

                        await this.connector.indexAbi(abiDocument); 
                    }
                }
            }

        }

        for (const tx of resp.block.transactions) {

            if (tx.trx[0] !== "packed_transaction")
                continue;

            const signatures = tx.trx[1].signatures;
            const packed_trx = tx.trx[1].packed_trx;

            try {
                const trx = deserialize(
                    this.reader.types, packed_trx);

                for (const action of trx.actions) {
                    // only care about eosio.evm::raw, eosio.evm::withdraw and
                    // transfers going to eosio.evm
                    const contractWhitelist = ["eosio.evm", "eosio.token"];
                    const actionWhitelist = ["raw", "withdraw", "transfer"]

                    if (!(action.account in contractWhitelist) ||
                        !(action.name in actionWhitelist))
                        continue;

                    const tx_data = Serialize.deserializeActionData(
                        this.getContract(action.account),
                        action.account,
                        action.name,
                        action.data,
                        encoder,
                        decoder);

                    let evmTx;

                    if (action.account == "eosio.evm") {
                        if (action.name == "raw") {
                            evmTx = await handleEvmTx(tx_data, signatures[0]);
                        } else if (action.name == "withdraw" ){
                            evmTx = await handleEvmWithdraw(tx_data, signatures[0]);
                        }
                    } else if (action.account == "eosio.token" &&
                            action.name == "transfer" &&
                            tx_data.to == "eosio.evm") {
                        evmTx = await handleEvmDeposit(tx_data, signatures[0]);
                    } else
                        return;

                    await this.connector.indexEvmTransaction(evmTx);
                    
                }

            } catch (error) {
                logger.error(getErrorMessage(error) + ": " + tx);
            }
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

        for (const accountName in this.abiWhitelist) {
            try {
                const abiDocument = await this.connector.getAbi(accountName);
                this.abis[abiDocument.account] = getContract(abiDocument.abi);

            } catch (error) {
                logger.warn(error);
            }
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
