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
