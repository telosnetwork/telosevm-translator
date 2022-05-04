import StateHistoryBlockReader from './ship';
import {
    ShipBlock,
    ShipBlockResponse,
    ShipTableDelta,
    ShipTransactionTrace
} from './types/ship';

const endpoint = 'ws://api2.hosts.caleos.io:8999';
const startBlock = 180698860;
const stopBlock = 0xffffffff;

const reader = new StateHistoryBlockReader(endpoint);

reader.setOptions({
    min_block_confirmation: 1,
    ds_threads: 32,
    allow_empty_deltas: false,
    allow_empty_traces: false,
    allow_empty_blocks: false
});

async function consumer(resp: ShipBlockResponse): Promise<void> {
    console.log(resp);
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
