import uWS, {TemplatedApp} from "uWebSockets.js";

import {BroadcasterConfig, IndexedBlockInfo} from "./types/indexer";
import {Bloom} from "./utils/evm";

import {NEW_HEADS_TEMPLATE, numToHex} from "./utils/evm";

import logger from './utils/winston';


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
                message: () => {},
                drain: () => {},
                close: () => {},
            }).listen(host, port, (token) => {
                if (token) {
                    logger.info('Listening to port ' + port);
                } else {
                    logger.error('Failed to listen to port ' + port);
                }
        });
    }

    convertTimestampToEpoch(timestamp: string) : number {
        return Math.floor(new Date(timestamp).getTime() / 1000);
    }

    broadcastBlock(blockInfo: IndexedBlockInfo) {
        let gasUsed = 0;
        let logsBloom = new Bloom();

        for (const tx of blockInfo.transactions) {
            gasUsed = tx['@raw'].gasusedblock;
            if (tx['@raw'].logsBloom) {
                logsBloom = new Bloom(
                    Buffer.from(tx['@raw'].logsBloom, 'hex'));
            }

            this.broadcastData('raw', tx);
        }

        const head = Object.assign({}, NEW_HEADS_TEMPLATE, {
            gasUsed: gasUsed,
            logsBloom: `0x${logsBloom.bitvector.toString("hex")}`,
            number: numToHex(blockInfo.delta['@global'].block_num),
            parentHash: '0x00',
            timestamp: `0x${this.convertTimestampToEpoch(blockInfo.delta['@timestamp']).toString(16)}`,
        })

        this.broadcastData('head', JSON.stringify(head));
    }

    private broadcastData(type: string, data: any) {
        this.broadcastServer.publish('broadcast', JSON.stringify({type, data}));
    }
}
