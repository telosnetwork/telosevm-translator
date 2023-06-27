import uWS, {TemplatedApp} from "uWebSockets.js";

import {BroadcasterConfig, IndexedBlockInfo} from "./types/indexer.js";
import {NEW_HEADS_TEMPLATE, numToHex} from "./utils/evm.js";

import logger from './utils/winston.js';


export default class RPCBroadcaster {

    config: BroadcasterConfig;
    broadcastServer: TemplatedApp

    constructor(config: BroadcasterConfig) {
        this.config = config;
        this.initUWS();
    }

    initUWS() {
        const host = this.config.wsHost;
        const port = this.config.wsPort;
        this.broadcastServer = uWS.App({}).ws(
            '/evm',
            {
                compression: 0,
                maxPayloadLength: 16 * 1024 * 1024,
                /* We need a slightly higher timeout for this crazy example */
                idleTimeout: 60,
                open: (ws) => ws.subscribe('broadcast'),
                message: () => {
                },
                drain: () => {
                },
                close: () => {
                },
            }).listen(host, port, (token) => {
            if (token) {
                logger.info('Listening to port ' + port);
            } else {
                logger.error('Failed to listen to port ' + port);
            }
        });
    }

    convertTimestampToEpoch(timestamp: string): number {
        return Math.floor(new Date(timestamp).getTime() / 1000);
    }

    broadcastBlock(blockInfo: IndexedBlockInfo) {
        let gasUsed = 0;

        if (blockInfo.transactions.length > 0)
            gasUsed = parseInt(blockInfo.transactions[blockInfo.transactions.length - 1]['@raw'].gasusedblock, 10);

        const head = Object.assign({}, NEW_HEADS_TEMPLATE, {
            parentHash: `0x${blockInfo.parentHash}`,
            extraData: `0x${blockInfo.nativeHash}`,
            receiptsRoot: `0x${blockInfo.receiptsRoot}`,
            transactionsRoot: `0x${blockInfo.delta['@transactionsRoot']}`,

            gasUsed: gasUsed,
            logsBloom: `0x${blockInfo.blockBloom}`,
            number: numToHex(blockInfo.delta['@global'].block_num),
            timestamp: `0x${this.convertTimestampToEpoch(blockInfo.delta['@timestamp']).toString(16)}`,
        })

        for (let trx in blockInfo.transactions)
            this.broadcastData('raw', JSON.stringify(trx));

        this.broadcastData('head', JSON.stringify(head));
    }

    private broadcastData(type: string, data: any) {
        this.broadcastServer.publish('broadcast', JSON.stringify({type, data}));
    }
}
