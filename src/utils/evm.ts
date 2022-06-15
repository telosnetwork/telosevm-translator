import { EvmTransaction } from '../types/evm';


const ethTrx = require('ethereumjs-tx');
const Common = require('ethereumjs-common').default;

const common = Common.forCustomChain(
  'mainnet',
  {name: 'Telos mainnet', networkId: 40, chainId: 40},
  'petersburg',
)

export function removeHexPrefix(str: string) {
    if (str.startsWith('0x')) {
        return str.slice(2)
    } else {
        return str
    }
}

export function getEvmTxHash(tx: EvmTransaction) {
    return new ethTrx.FakeTransaction(
        tx, {common}).hash().toString('hex');
}


const BN = require('bn.js');

import assert from "assert";
import { zeros, keccak256 } from "ethereumjs-util";

const BYTE_SIZE = 256;

export default class Bloom {
  bitvector: Buffer;

  /**
   * Represents a Bloom filter.
   */
  constructor(bitvector?: Buffer) {
    if (!bitvector) {
      this.bitvector = zeros(BYTE_SIZE);
    } else {
      assert(
        bitvector.length === BYTE_SIZE,
        "bitvectors must be 2048 bits long"
      );
      this.bitvector = bitvector;
    }
  }

  /**
   * Adds an element to a bit vector of a 64 byte bloom filter.
   * @param e - The element to add
   */
  add(e: Buffer) {
    assert(Buffer.isBuffer(e), "Element should be buffer");
    e = keccak256(e);
    const mask = 2047; // binary 11111111111

    for (let i = 0; i < 3; i++) {
      const first2bytes = e.readUInt16BE(i * 2);
      const loc = mask & first2bytes;
      const byteLoc = loc >> 3;
      const bitLoc = 1 << loc % 8;
      this.bitvector[BYTE_SIZE - byteLoc - 1] |= bitLoc;
    }
  }

  /**
   * Checks if an element is in the bloom.
   * @param e - The element to check
   */
  check(e: Buffer): boolean {
    assert(Buffer.isBuffer(e), "Element should be Buffer");
    e = keccak256(e);
    const mask = 2047; // binary 11111111111
    let match = true;

    for (let i = 0; i < 3 && match; i++) {
      const first2bytes = e.readUInt16BE(i * 2);
      const loc = mask & first2bytes;
      const byteLoc = loc >> 3;
      const bitLoc = 1 << loc % 8;
      match = (this.bitvector[BYTE_SIZE - byteLoc - 1] & bitLoc) !== 0;
    }

    return Boolean(match);
  }

  /**
   * Checks if multiple topics are in a bloom.
   * @returns `true` if every topic is in the bloom
   */
  multiCheck(topics: Buffer[]): boolean {
    return topics.every((t: Buffer) => this.check(t));
  }

  /**
   * Bitwise or blooms together.
   */
  or(bloom: Bloom) {
    if (bloom) {
      for (let i = 0; i <= BYTE_SIZE; i++) {
        this.bitvector[i] = this.bitvector[i] | bloom.bitvector[i];
      }
    }
  }
}



const ZERO_ADDR = '0x0000000000000000000000000000000000000000';
const NULL_HASH = '0x0000000000000000000000000000000000000000000000000000000000000000';
const EMPTY_LOGS = '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';

// 1,000,000,000
const BLOCK_GAS_LIMIT = '0x3b9aca00'

const NEW_HEADS_TEMPLATE =
    {
        difficulty: "0x0",
        extraData: NULL_HASH,
        gasLimit: BLOCK_GAS_LIMIT,
        miner: ZERO_ADDR,
        nonce: "0x0000000000000000",
        parentHash: NULL_HASH,
        receiptsRoot: NULL_HASH,
        sha3Uncles: NULL_HASH,
        stateRoot: NULL_HASH,
        transactionsRoot: NULL_HASH,
    };

const BLOCK_TEMPLATE =
    Object.assign({
        mixHash: NULL_HASH,
        size: "0x0",
        totalDifficulty: "0x0",
        uncles: []
    }, NEW_HEADS_TEMPLATE);

export { BLOCK_TEMPLATE, NEW_HEADS_TEMPLATE, EMPTY_LOGS }


export function numToHex(input: number | string) {
    if (typeof input === 'number') {
        return '0x' + input.toString(16)
    } else {
        return '0x' + new BN(input).toString(16)
    }
}
