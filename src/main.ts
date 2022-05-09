import StateHistoryBlockReader from './ship';
import {
    ShipBlock,
    ShipBlockResponse,
    ShipTableDelta,
    ShipTransactionTrace
} from './types/ship';

import {
    AbiDocument
} from './types/eosio';

import * as eosioEvmAbi from './abis/evm.json';
import * as eosioTokenAbi from './abis/token.json'

import logger from './utils/winston';

import { Serialize , RpcInterfaces } from 'eosjs';

import { handleEvmTx, handleEvmDeposit, handleEvmWithdraw } from './handlers';

import { ElasticConnector } from './database';


const encoder = new TextEncoder;
const decoder = new TextDecoder;

const abiWhitelist = [ 'eosio.evm', 'eosio.token' ];

const connector = new ElasticConnector();

function getContract(contractAbi: RpcInterfaces.Abi) {
    const types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), contractAbi)
    const actions = new Map()
    for (const { name, type } of contractAbi.actions) {
        actions.set(name, Serialize.getType(types, type))
    }
    return { types, actions }
}

const endpoint = 'ws://api2.hosts.caleos.io:8999';
const startBlock = 200000000; // 180698860;
const stopBlock = 0xffffffff;

let currentBlock = startBlock;

const reader = new StateHistoryBlockReader(endpoint);

reader.setOptions({
    min_block_confirmation: 1,
    ds_threads: 8,
    allow_empty_deltas: false,
    allow_empty_traces: false,
    allow_empty_blocks: false
});


const evmContract = getContract(eosioEvmAbi.abi);
const tokenContract = getContract(eosioTokenAbi.abi);

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

async function consumer(resp: ShipBlockResponse): Promise<void> {
    for (const tx of resp.block.transactions) {
        if (tx.trx[0] == "packed_transaction") {
            const signatures = tx.trx[1].signatures;
            const packed_trx = tx.trx[1].packed_trx;

            try {
                const trx = deserialize(reader.types, packed_trx);
                
                for (const action of trx.actions) {
                    let contract = null;

                    // handle abi update
                    if (action.account == "eosio" &&
                        action.name == "setabi") {
                        console.log(action);
                        console.log(trx);
                        process.exit(1)
                    }

                    // only care about eosio.evm::raw, eosio.evm::withdraw and
                    // transfers going to eosio.evm

                    if (action.account == "eosio.evm" &&
                        (action.name == "raw" || action.name == "withdraw")) {
                        contract = evmContract;

                    } else if (action.account == "eosio.token" && action.name == "transfer") {
                        contract = tokenContract;
                    } else {
                        return;
                    }

                    const tx_data = Serialize.deserializeActionData(
                        contract,
                        action.account,
                        action.name,
                        action.data,
                        encoder,
                        decoder);

                    let evmTx;

                    if (action.account == "eosio.evm") {
                        if (action.name == "raw") {
                            evmTx = await handleEvmTx(tx_data, signatures[0]);
                        } else {
                            evmTx = await handleEvmWithdraw(tx_data, signatures[0]);
                        }
                    } else if (action.account == "eosio.token" &&
                               action.name == "transfer" &&
                               tx_data.to == "eosio.evm") {
                        evmTx = await handleEvmDeposit(tx_data, signatures[0]);
                    }

                    logger.info(JSON.stringify(evmTx));
                    await connector.indexEvmTransaction(evmTx);
                    
                }

            } catch (error) {
                logger.error(getErrorMessage(error) + ": " + tx);
            }
        }
    }
}

reader.consume(consumer);

reader.startProcessing({
    start_block_num: startBlock,
    end_block_num: stopBlock,
    max_messages_in_flight: 10,
    irreversible_only: true,
    have_positions: [],
    fetch_block: true,
    fetch_traces: true,
    fetch_deltas: true
}, ['contract_row']);
