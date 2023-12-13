import uWS, {TemplatedApp} from "uWebSockets.js";

import {BroadcasterConfig, IndexedBlockInfo} from "./types/indexer.js";
import {NEW_HEADS_TEMPLATE, numToHex} from "./utils/evm.js";
import {Logger} from "winston";


export default class RPCBroadcaster {

    config: BroadcasterConfig;
    logger: Logger;
    broadcastServer: TemplatedApp;

    private sockets: {[key: string]: uWS.WebSocket};
    private listenSocket: uWS.us_listen_socket;

    constructor(config: BroadcasterConfig, logger: Logger) {
        this.config = config;
        this.logger = logger;
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
                open: (ws) => {
                    const ip: string = new TextDecoder('utf-8').decode(ws.getRemoteAddressAsText());
                    this.sockets[ip] = ws;
                    ws.subscribe('broadcast')
                },
                message: () => {
                },
                drain: () => {
                },
                close: (ws) => {
                    const ip: string = new TextDecoder('utf-8').decode(ws.getRemoteAddressAsText());
                    if (ip in this.sockets)
                        delete this.sockets[ip];
                },
            }).listen(host, port, (listenSocket) => {
            if (listenSocket) {
                this.listenSocket = listenSocket;
                this.logger.info('Listening to port ' + port);
            } else {
                this.logger.error('Failed to listen to port ' + port);
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

            gasUsed: numToHex(gasUsed),
            logsBloom: `0x${blockInfo.blockBloom}`,
            number: numToHex(blockInfo.delta['@global'].block_num),
            timestamp: `0x${this.convertTimestampToEpoch(blockInfo.delta['@timestamp']).toString(16)}`,
        })

        for (let trx of blockInfo.transactions)
            this.broadcastData('raw', trx);

        this.broadcastData('head', head);
    }

    private broadcastData(type: string, data: any) {
        this.broadcastServer.publish('broadcast', JSON.stringify({type, data}));
    }

    close() {
        for (const ip in this.sockets)
            this.sockets[ip].close();

        uWS.us_listen_socket_close(this.listenSocket);
    }
}
