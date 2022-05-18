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
import * as eosioMsigAbi from './abis/msig.json'
import * as eosioTokenAbi from './abis/token.json'
import * as eosioSystemAbi from './abis/system.json'

import logger from './utils/winston';

import { Serialize , RpcInterfaces } from 'eosjs';

import { handleEvmTx, handleEvmDeposit, handleEvmWithdraw } from './handlers';

import { ElasticConnector } from './database';

// eosio rpc client
const fetch = require('isomorphic-fetch');
import { hexToUint8Array } from 'eosjs/dist/eosjs-serialize';
import { Api, JsonRpc } from 'eosjs/dist';
import { JsSignatureProvider } from 'eosjs/dist/eosjs-jssig';
import { Abi } from 'eosjs/dist/eosjs-rpc-interfaces';

import { IndexerStateDocument } from './types/indexer';

const encoder = new TextEncoder;
const decoder = new TextDecoder;
const abiTypes = Serialize.getTypesFromAbi(Serialize.createAbiTypes());

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

function rawAbiToJson(rawAbi: Uint8Array): Abi {
    const buffer = new Serialize.SerialBuffer({
        textEncoder: encoder,
        textDecoder: decoder,
        array: rawAbi,
    });
    if (!Serialize.supportedAbiVersion(buffer.getString())) {
        throw new Error('Unsupported abi version');
    }
    buffer.restartRead();
    return abiTypes.get('abi_def').deserialize(buffer);
}


function getErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message
  return String(error)
}


export class TEVMIndexer {

    endpoint: string;
    abiWhitelist: string[];
    defaultAbis: any;

    currentBlock: number;
    startBlock: number;
    stopBlock: number;

    reader: StateHistoryBlockReader;
    telosApi: Api;
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

        this.telosApi = new Api({
            rpc: new JsonRpc("https://mainnet.telos.net", { fetch }),
            signatureProvider: new JsSignatureProvider([])
        });
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
            evm: getContract(eosioEvmAbi.abi),
            msig: getContract(eosioMsigAbi.abi),
            token: getContract(eosioTokenAbi.abi),
            system: getContract(eosioSystemAbi.abi)
        };
    }

    async consumer(resp: ShipBlockResponse): Promise<void> {

        this.currentBlock = resp.this_block.block_num;
        if (this.currentBlock % 1000 == 0)
            logger.info(this.currentBlock + " indexed, ")

        for (const delta of resp.deltas) {

            if (delta[0] != "table_delta_v0")
                continue;

            const name = delta[1].name;
            const rows = delta[1].rows;

            if (name == "account") {
                for (const row of rows) {
                    if (row.present) {
                        console.log(row);
                        // @ts-ignore
                        const data: Uint8Array = row.data;
                        const newAbi = rawAbiToJson(data);
                        console.log(JSON.stringify(newAbi, null, 4))
                        // const abiDocument = {
                        //     block_num: this.currentBlock,
                        //     timestamp: resp.block.timestamp,
                        //     data: {
                        //         abi: newAbi
                        //     }
                        // };

                        // await this.connector.indexAbiProposal(abiDocument); 
                    }
                }
            }

        }

        process.exit(0);

        // for (const tx of resp.block.transactions) {

        //     if (tx.trx[0] !== "packed_transaction")
        //         continue;

        //     const signatures = tx.trx[1].signatures;
        //     const packed_trx = tx.trx[1].packed_trx;

        //     try {
        //         const trx = deserialize(
        //             this.reader.types, packed_trx);

        //         for (const action of trx.actions) {
        //             let contract = null;

        //             // check eosio.msigs for abi updates & for evm:
        //             // only care about eosio.evm::raw, eosio.evm::withdraw and
        //             // transfers going to eosio.evm

        //             if (action.account == "eosio.msig" &&
        //                 (action.name == "exec" || action.name == "propose")) {
        //                 contract = this.defaultAbis.msig;

        //             } else if (action.account == "eosio.evm" &&
        //                 (action.name == "raw" || action.name == "withdraw")) {
        //                 contract = this.defaultAbis.evm;

        //             } else if (action.account == "eosio.token" && action.name == "transfer") {
        //                 contract = this.defaultAbis.token;
        //             } else {
        //                 return;
        //             }

        //             const tx_data = Serialize.deserializeActionData(
        //                 contract,
        //                 action.account,
        //                 action.name,
        //                 action.data,
        //                 encoder,
        //                 decoder);

        //             // handle msig abi updates
        //             if (action.account == "eosio.msig") {

        //                 if (action.name == "propose") {
    
        //                     for (const sub_action of tx_data.trx.actions) {
        //                         if (sub_action.account == "eosio" &&
        //                             sub_action.name == "setabi") {
        //                             
        //                             const stx_data = Serialize.deserializeActionData(
        //                                 this.defaultAbis.system,
        //                                 sub_action.account, sub_action.name, sub_action.data,
        //                                 encoder, decoder);

        //                             if (!this.abiWhitelist.includes(stx_data.account))
        //                                 return;

        //                             const newAbi = this.telosApi.rawAbiToJson(
        //                                 hexToUint8Array(stx_data.abi));

        //                             const abiDocument = {
        //                                 block_num: this.currentBlock,
        //                                 proposal_name: tx_data.proposal_name,
        //                                 timestamp: resp.block.timestamp,
        //                                 data: {
        //                                     account: stx_data.account,
        //                                     abi: newAbi
        //                                 }
        //                             };

        //                             await this.connector.indexAbiProposal(abiDocument); 
        //                         }
        //                     }
        //                 
        //                 } else if (action.name == "exec") {
        //                     const matchingProposals = await this.connector.getAbiProposal(
        //                         tx_data.proposal_name);

        //                     if (matchingProposals.length > 0) {
        //                         const prop = matchingProposals[0]._source;
        //                         await this.connector.indexAbi(prop); 
        //                     }
        //                 }

        //                 return;
        //             }

        //             let evmTx;

        //             if (action.account == "eosio.evm") {
        //                 if (action.name == "raw") {
        //                     evmTx = await handleEvmTx(tx_data, signatures[0]);
        //                 } else if (action.name == "withdraw" ){
        //                     evmTx = await handleEvmWithdraw(tx_data, signatures[0]);
        //                 }
        //             } else if (action.account == "eosio.token" &&
        //                     action.name == "transfer" &&
        //                     tx_data.to == "eosio.evm") {
        //                 evmTx = await handleEvmDeposit(tx_data, signatures[0]);
        //             } else
        //                 return;

        //             await this.connector.indexEvmTransaction(evmTx);
        //             
        //         }

        //     } catch (error) {
        //         logger.error(getErrorMessage(error) + ": " + tx);
        //     }
        // }
    }

    async launch() {

        let prevState = null;
            
        try {
            prevState = await this.connector.getIndexerState();
        } catch (error) {
            logger.warn(error);
        }

        logger.info(prevState);

        this.reader.consume(this.consumer.bind(this));

        this.reader.startProcessing({
            start_block_num: this.startBlock,
            end_block_num: this.stopBlock,
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
