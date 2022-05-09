import {
    EosioEvmRaw,
    EosioEvmDeposit,
    EosioEvmWithdraw,
    EvmTransaction
} from './types/evm';

import { parseAsset } from './utils/eosio';
import logger from './utils/winston';

const {Signature} = require('eosjs-ecc');

// ethereum tools
var txDecoder = require('ethereum-tx-decoder');
var Units = require('ethereumjs-units');

const BN = require('bn.js')


export async function handleEvmTx(tx: EosioEvmRaw, nativeSig: string) {
    // const evmTx = new ethTrx.Transaction(
    //     `0x${tx.tx.toLowerCase()}`, {common});

    const evmTx = txDecoder.decodeTx(`0x${tx.tx.toLowerCase()}`);

    if (evmTx.v == 0) {
        const sig = Signature.fromString(nativeSig);
        evmTx.v = `0x${(27).toString(16).padStart(64, '0')}`;
        evmTx.r = `0x${sig.r.toHex().padStart(64, '0')}`;
        evmTx.s = `0x${sig.s.toHex()}`;
    }

    // logger.info(JSON.stringify(evmTx));

    return evmTx;
}

const stdGasPrice = "0x7a307efa80";
const stdGasLimit = `0x${(21000).toString(16)}`;

export async function handleEvmDeposit(tx: EosioEvmDeposit, nativeSig: string) {

    const quantity = parseAsset(tx.quantity);
    const quantWei = Units.convert(quantity.amount, 'eth', 'wei');

    const sig = Signature.fromString(nativeSig);
    const evmTx = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: tx.memo,
        value: `0x${new BN(quantWei, 16)._strip()}`,
        data: "0x",
        v: `0x${(27).toString(16).padStart(64, '0')}`,
        r: `0x${sig.r.toHex().padStart(64, '0')}`,
        s: `0x${sig.s.toHex()}`
    };

    // logger.warn(JSON.stringify(evmTx));

    return evmTx; // new ethTrx.Transaction(evmTx, {common});
}

export async function handleEvmWithdraw(tx: EosioEvmWithdraw, nativeSig: string) {

    const quantity = parseAsset(tx.quantity);
    const quantWei = Units.convert(quantity.amount, 'eth', 'wei');

    const sig = Signature.fromString(nativeSig);
    const evmTx = {
        nonce: 0,
        gasPrice: stdGasPrice,
        gasLimit: stdGasLimit,
        to: "0x0000000000000000000000000000000000000000",
        value: `0x${new BN(quantWei, 16)._strip()}`,
        data: "0x",
        v: `0x${(27).toString(16).padStart(64, '0')}`,
        r: `0x${sig.r.toHex().padStart(64, '0')}`,
        s: `0x${sig.s.toHex()}`
    };

    // logger.error(JSON.stringify(evmTx));

    return  evmTx; // new ethTrx.Transaction(evmTx, {common});
}
