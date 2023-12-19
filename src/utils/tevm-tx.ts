import {LegacyTransaction, LegacyTxData, TransactionType} from "@ethereumjs/tx";
import type { Common } from '@ethereumjs/common';

import type {
    TxValuesArray as AllTypesTxValuesArray,
    TxOptions,
} from '@ethereumjs/tx';
import RLP from "rlp";

type TxValuesArray = AllTypesTxValuesArray[TransactionType.Legacy]

export class TEVMTransaction extends LegacyTransaction {

    /**
     * Instantiate a transaction from a data dictionary.
     *
     * Format: { nonce, gasPrice, gasLimit, to, value, data, v, r, s }
     *
     * Notes:
     * - All parameters are optional and have some basic default values
     */
    public static fromTxData(txData: LegacyTxData, opts: TxOptions = {}) {
        return new TEVMTransaction(txData, opts)
    }

    /**
     * Instantiate a transaction from the serialized tx.
     *
     * Format: `rlp([nonce, gasPrice, gasLimit, to, value, data, v, r, s])`
     */
    public static fromSerializedTx(serialized: Uint8Array, opts: TxOptions = {}) {
        let values = RLP.decode(serialized)

        if (!Array.isArray(values)) {
            if (values.hasOwnProperty('data')) {
                // @ts-ignore
                values = values.data;
            } else
                throw new Error('Invalid serialized tx input. Must be array')
        }

        return this.fromValuesArray(values as TxValuesArray, opts)
    }

    /**
     * Create a transaction from a values array.
     *
     * Format: `[nonce, gasPrice, gasLimit, to, value, data, v, r, s]`
     */
    public static fromValuesArray(values: TxValuesArray, opts: TxOptions = {}) {
        // If length is not 6, it has length 9. If v/r/s are empty Uint8Arrays, it is still an unsigned transaction
        // This happens if you get the RLP data from `raw()`
        if (values.length !== 6 && values.length !== 9) {
            // TELOS: allow for bigger than 9 values.length but only if no trailing bytes
            if (values.length > 10) {
                let i = 10;
                while (i < values.length) {
                    if (values[i].length !== 0) {
                        throw new Error(
                            'Invalid transaction. Only expecting 6 values (for unsigned tx) or 9 values (for signed tx).'
                        )
                    }
                }
            }
        }

        const [nonce, gasPrice, gasLimit, to, value, data, v, r, s] = values

        // TELOS: dont validate leading zeros
        // validateNoLeadingZeroes({ nonce, gasPrice, gasLimit, value, v, r, s })

        return new TEVMTransaction(
            {
                nonce,
                gasPrice,
                gasLimit,
                to,
                value,
                data,
                v,
                r,
                s,
            },
            opts
        )
    }

    public isSigned(): boolean {
        const { v, r, s } = this
        if ((v === undefined || r === undefined || s === undefined)) {
            return false;
        } else if ((v == this.common.chainId()) && (r === BigInt(0)) && (s === BigInt(0))) {
            return false;
        } else if ((v === BigInt(0)) && (r === BigInt(0)) && (s === BigInt(0))) {
            return false;
        } else {
            return true;
        }
    }
    
    /**
     * Validates tx's `v` value
     */
    protected _validateTxV(_v?: bigint, common?: Common): Common {
        return common;
    }

}