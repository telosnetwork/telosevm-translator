import { TEVMIndexer } from './indexer';


const endpoint = 'ws://api2.hosts.caleos.io:8999';
const startBlock = 200000000; // 180698860;
const stopBlock = 0xffffffff;


const indexer = new TEVMIndexer(
    endpoint, startBlock, stopBlock);

indexer.launch();
