import { TEVMIndexer } from './indexer';


const endpoint = 'ws://api2.hosts.caleos.io:8999';
const startBlock = 180698860; // 180698860;
const stopBlock = 0xffffffff;


const indexer = new TEVMIndexer(
    endpoint,
    ['eosio', 'eosio.evm', 'eosio.token'],
    startBlock, stopBlock
);

indexer.launch();
