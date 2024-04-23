import uWS, {TemplatedApp} from "uWebSockets.js";
import { v4 as uuidv4 } from 'uuid';

import {arrayToHex, NEW_HEADS_TEMPLATE, numToHex} from "./utils/evm.js";
import {Logger} from "winston";
import {BroadcasterConfig} from "./types/config.js";
import {IndexedBlock} from "./types/indexer.js";


export default class RPCBroadcaster {

    config: BroadcasterConfig;
    logger: Logger;
    broadcastServer: TemplatedApp;

    private sockets: {[key: string]: uWS.WebSocket<Uint8Array>} = {};
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
                open: (ws: uWS.WebSocket<Uint8Array>) => {
                    const uuid = uuidv4();
                    // @ts-ignore
                    ws.uuid = uuid;
                    this.sockets[uuid] = ws;
                    ws.subscribe('broadcast')
                },
                message: () => {
                },
                drain: () => {
                },
                close: (ws) => {
                    // @ts-ignore
                    const uuid = ws.uuid;
                    if (uuid && uuid in this.sockets)
                        delete this.sockets[uuid];
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

    broadcastBlock(block: IndexedBlock) {
        const head = Object.assign({}, NEW_HEADS_TEMPLATE, {
            parentHash: `0x${arrayToHex(block.evmPrevHash)}`,
            extraData: `0x${arrayToHex(block.blockHash)}`,
            receiptsRoot: `0x${arrayToHex(block.receiptsRoot)}`,
            transactionsRoot: `0x${arrayToHex(block.transactionsRoot)}`,
            hash: `0x${arrayToHex(block.evmBlockHash)}`,
            gasUsed: numToHex(block.gasUsed),
            logsBloom: `0x${arrayToHex(block.logsBloom)}`,
            number: numToHex(block.evmBlockNum),
            timestamp: numToHex(block.timestamp),
        })

        for (let trx of block.transactions)
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
