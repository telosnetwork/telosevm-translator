import StateHistoryBlockReader from './ship';
import {
    ShipBlock,
    ShipBlockResponse,
    ShipTableDelta,
    ShipTransactionTrace
} from './types/ship';

import * as eosioEvmAbi from './abis/evm.json';
import * as eosioTokenAbi from './abis/token.json'

import { Serialize , RpcInterfaces } from 'eosjs';

const encoder = new TextEncoder;
const decoder = new TextDecoder;


function getContract(contractAbi: RpcInterfaces.Abi) {
    const types = Serialize.getTypesFromAbi(Serialize.createInitialTypes(), contractAbi)
    const actions = new Map()
    for (const { name, type } of contractAbi.actions) {
        actions.set(name, Serialize.getType(types, type))
    }
    return { types, actions }
}

const endpoint = 'ws://api2.hosts.caleos.io:8999';
const startBlock = 180698860;
const stopBlock = 0xffffffff;

const reader = new StateHistoryBlockReader(endpoint);

reader.setOptions({
    min_block_confirmation: 1,
    ds_threads: 16,
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

async function consumer(resp: ShipBlockResponse): Promise<void> {
    resp.block.transactions.forEach((tx) => {
        if (tx.trx[0] == "packed_transaction") {
            const packed_trx = tx.trx[1].packed_trx;
            const trx = deserialize(reader.types, packed_trx);
            
            trx.actions.forEach((action: Serialize.Action) => {
                let contract = null;
                if (action.account == "eosio.evm") {
                    contract = evmContract;
                } else if (action.account == "eosio.token") {
                    contract = tokenContract;
                } else {
                    return;
                }

                const result = Serialize.deserializeActionData(
                    contract,
                    action.account,
                    action.name,
                    action.data,
                    encoder,
                    decoder);

                console.log(trx);
                console.log(result);
            });
        }
    });
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
