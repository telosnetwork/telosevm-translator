import {IndexedBlock} from "../types/indexer.js";
import {arrayToHex, EMPTY_TRIE_BUF, removeHexPrefix, ZERO_HASH, ZERO_HASH_BUF} from "../utils/evm.js";
import {Bloom} from "@ethereumjs/vm";
import {mergeDeep} from "../utils/misc.js";
import {undefined} from "zod";

export const sampleActionDocument = {
    "@timestamp": "2022-05-02T22:42:51.500",
    "trx_id": "9ab685d7447f800fd9291002081521f71b54c92c919793339592a27ca3c3e1ea",
    "action_ordinal": 1,
    "signatures": [],
    "@raw": {
        "hash": "0x4037ccf3a670073d171d6230e6289fdbef678a2af7dfd19c9cc5c0182df1afdc",
        "trx_index": 0,
        "block": 213195255,
        "block_hash": "745d46b1522cbb24c676c985b1bf189ea91c881653e15551ee5cd9477ad4a489",
        "to": "0x86c5997d10d1df6cbb15c01280e596ed5ac90a33",
        "input_data": "0x7898e0c2000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000042a383736e0000000000000000000000000000000000000000000000000000000062705e6a00000000000000000000000000000000000000000000000000000000000000074554482f55534400000000000000000000000000000000000000000000000000",
        "input_trimmed": "0x7898e0c2000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000042a383736e0000000000000000000000000000000000000000000000000000000062705e6a00000000000000000000000000000000000000000000000000000000000000074554482f55534400000000000000000000000000000000000000000000000000",
        "value": "0",
        "value_d": "0",
        "nonce": "184",
        "gas_price": "549790097103",
        "gas_limit": "1000725",
        "status": 1,
        "itxs": [],
        "epoch": 1651531371,
        "createdaddr": "",
        "gasused": "32512",
        "gasusedblock": "32512",
        "charged_gas_price": "499809179185",
        "output": "",
        "v": "115",
        "r": "0xef51515977e65f0390a6a58924dc06b535fcd0c900b51edda7faf6314b079d75",
        "s": "0x371bcd1c22efc77d3cdb9302631e6d82189e62365bf98417174337c4646d124b",
        "from": "0x4b4c4a6e760fdea078efc6bea046946b639bddea",
        "logs": [
            {
                "address": "86c5997d10d1df6cbb15c01280e596ed5ac90a33",
                "data": "000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000042a383736e0000000000000000000000000000000000000000000000000000000062705e6a00000000000000000000000000000000000000000000000000000000000000074554482f55534400000000000000000000000000000000000000000000000000",
                "topics": [
                    "a7fc99ed7617309ee23f63ae90196a1e490d362e6f6a547a59bc809ee2291782"
                ]
            }
        ],
        "logsBloom": "00000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000004000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    }
};

export const sampleDeltaDocument = {
    "@timestamp": "2022-05-04T03:48:49Z",
    "block_num": 213404649,
    "@global": {
        "block_num": 213404613
    },
    "@blockHash": "0cb84be93c45e24ac66b80f31701181cce97f9ef0e6a5757f488bbb81a6391a7",
    "@evmBlockHash": "3762dfc2b4580f7d6dd5531fe7b8f2abd0662f6d863359ba445df82c7829ef5d",
    "@evmPrevBlockHash": "1b7e30e80250d0eb04815eaeed2f28725605edc3e0c9b4d9b92408a4b44ae4f7",
    "@receiptsRootHash": "0e742ac48fb89089d1c04a35357f60fe23aa73836122c08649e5fe7bd42d3ed0",
    "@transactionsRoot": "7701514ced0e7e9c38ec05d275bf4602e48bfaccdb7afd58ec6d368497b156ca",
    "gasUsed": "302137",
    "gasLimit": "433922",
    "size": "238",
    "code": "eosio",
    "table": "global"
};

function getBlockTimeFrom(from: Date): Date {
    const roundedNow = Math.ceil(from.getTime() / 500) * 500;
    return new Date(roundedNow);
}

export function sampleIndexedBlock(block: Partial<IndexedBlock>, opts: {
    chainStartTime?: Date
}): IndexedBlock {
    const defaultBlock: IndexedBlock = {
        blockHash: ZERO_HASH_BUF,
        blockNum: 0n,
        deltas: {account: [], accountstate: []},
        evmBlockHash: ZERO_HASH_BUF,
        evmBlockNum: 0n,
        evmPrevHash: ZERO_HASH_BUF,
        gasLimit: 0n,
        gasUsed: 0n,
        logsBloom: (new Bloom()).bitvector,
        receiptsRoot: EMPTY_TRIE_BUF,
        size: 0n,
        timestamp: 0n,
        transactionAmount: 0,
        transactions: [],
        transactionsRoot: EMPTY_TRIE_BUF
    };
    const sample: IndexedBlock = mergeDeep(defaultBlock, block);

    if (opts.chainStartTime) {
        const blockSecondOffset = sample.blockNum / 2n;
        const adjustedTime = BigInt(opts.chainStartTime.getTime()) + blockSecondOffset;
        sample.timestamp = adjustedTime;
    }

    return sample;
}
