import {
    EosioEvmRaw,
    EosioEvmDeposit,
    EosioEvmWithdraw
} from './types/evm';

const {Signature} = require('eosjs-ecc');

// ethereum tools
var txDecoder = require('ethereum-tx-decoder');

export async function handleEvmTx(tx: EosioEvmRaw, nativeSig: string) {
    const evmTx = txDecoder.decodeTx(`0x${tx.tx.toLowerCase()}`);

    if (evmTx.v == 0) {
        const sig = Signature.fromString(nativeSig);
        evmTx.v = `0x${(27).toString(16).padStart(64, '0')}`;
        evmTx.r = `0x${sig.r.toHex().padStart(64, '0')}`;
        evmTx.s = `0x${sig.s.toHex()}`;
    }

    console.log(evmTx);
}

export async function handleEvmDeposit(tx: EosioEvmDeposit) {
}

export async function handleEvmWithdraw(tx: EosioEvmWithdraw) {
}
