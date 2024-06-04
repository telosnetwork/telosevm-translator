import { z } from 'zod';
import {
    ArrowBatchCompression,
    ArrowBatchConfigSchema,
    DEFAULT_BUCKET_SIZE, DEFAULT_DUMP_SIZE
} from "@guilledk/arrowbatch-nodejs";
import {packageInfo} from "../utils/indexer.js";

// Custom type for bigint
const BigIntSchema = z.union([
    z.string(),
    z.number(),
    z.bigint()
]).transform((val, ctx) => {
    try {
        return BigInt(val);
    } catch {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'Invalid bigint',
        });
        return z.NEVER;
    }
});

// Custom type for Uint8Array (hex string)
const Uint8ArraySchema = z.string().transform((val, ctx) => {
    const stripped = val.startsWith('0x') ? val.slice(2) : val;
    if (stripped.length % 2 !== 0) {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'Invalid Uint8Array hex string',
        });
        return z.NEVER;
    }
    const bytes = new Uint8Array(stripped.length / 2);
    for (let i = 0; i < stripped.length; i += 2) {
        bytes[i / 2] = parseInt(stripped.substr(i, 2), 16);
    }
    return bytes;
});

const HashSchema = z.string().transform((val, ctx) => {
    const stripped = val.startsWith('0x') ? val.slice(2) : val;
    if (stripped.length === 0) {
        return new Uint8Array(32);
    }
    if (stripped.length !== 64) {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: 'Invalid hash length, expected 32 bytes',
        });
        return z.NEVER;
    }
    const bytes = new Uint8Array(32);
    for (let i = 0; i < 64; i += 2) {
        bytes[i / 2] = parseInt(stripped.substr(i, 2), 16);
    }
    return bytes;
});

const ElasticConnectorConfigSchema = z.object({
    node: z.string(),
    auth: z.object({
        username: z.string(),
        password: z.string(),
    }).optional(),
    requestTimeout: z.number(),
    docsPerIndex: z.number(),
    scrollSize: z.number().optional(),
    scrollWindow: z.string().optional(),
    numberOfShards: z.number().default(1),
    numberOfReplicas: z.number().default(0),
    refreshInterval: z.number().default(-1),
    codec: z.string().default('default'),
    dumpSize: z.number(),
    suffix: z.object({
        block: z.string(),
        error: z.string(),
        transaction: z.string(),
        fork: z.string(),
        account: z.string(),
        accountstate: z.string(),
    }),
});

const ChainConfigSchema = z.object({
    chainName: z.string(),
    chainId: z.number(),
    startBlock: BigIntSchema,
    stopBlock: BigIntSchema.optional(),
    evmBlockDelta: BigIntSchema,
    evmPrevHash: HashSchema.optional(),
    evmValidateHash: HashSchema.optional(),
    irreversibleOnly: z.boolean(),
});

const BroadcasterConfigSchema = z.object({
    wsHost: z.string(),
    wsPort: z.number(),
});

const ConnectorConfigSchema = z.object({
    chain: ChainConfigSchema.partial().optional(),
    elastic: ElasticConnectorConfigSchema.optional(),
    arrow: ArrowBatchConfigSchema.optional(),
    compatLevel: z.string().default(packageInfo.version),
    logLevel: z.string().optional(),
    trimFrom: z.number().optional(),
    skipIntegrityCheck: z.boolean().optional(),
    gapsPurge: z.boolean().optional(),
});

const SourceConnectorConfigSchema = ConnectorConfigSchema.extend({
    chain: ChainConfigSchema,
    nodeos: z.object({
        endpoint: z.string(),
        remoteEndpoint: z.string(),
        wsEndpoint: z.string(),
        blockHistorySize: z.number().default(1800),
        stallCounter: z.number(),
        evmWorkerAmount: z.number(),
        readerWorkerAmount: z.number(),
        maxMsgsInFlight: z.number().optional(),
        skipStartBlockCheck: z.boolean().optional(),
        skipRemoteCheck: z.boolean().optional(),
        maxMessagesInFlight: z.number().default(10000),
        maxWsPayloadMb: z.number().default(2048)
    }).optional(),
});

const TranslatorConfigSchema = z.object({
    source: SourceConnectorConfigSchema,
    target: ConnectorConfigSchema,
    logLevel: z.string(),
    readerLogLevel: z.string(),
    runtime: z.object({
        eval: z.boolean().optional(),
        timeout: z.number().optional(),
        onlyDBCheck: z.boolean().optional(),
    }),
    broadcast: BroadcasterConfigSchema,
});

export type ElasticConnectorConfig = z.infer<typeof ElasticConnectorConfigSchema>;
export type ArrowConnectorConfig = z.infer<typeof ArrowBatchConfigSchema>;
export type ChainConfig = z.infer<typeof ChainConfigSchema>;
export type BroadcasterConfig = z.infer<typeof BroadcasterConfigSchema>;
export type ConnectorConfig = z.infer<typeof ConnectorConfigSchema>;
export type SourceConnectorConfig = z.infer<typeof SourceConnectorConfigSchema>;
export type TranslatorConfig = z.infer<typeof TranslatorConfigSchema>;

export const DEFAULT_CONF = TranslatorConfigSchema.parse({
    source: {
        chain: {
            chainName: 'telos-local',
            chainId: 41,
            startBlock: '35',
            evmBlockDelta: 2,
            evmPrevHash: '',
            evmValidateHash: '',
            irreversibleOnly: false,
        },
        nodeos: {
            endpoint: 'http://127.0.0.1:8888',
            remoteEndpoint: 'http://127.0.0.1:8888',
            wsEndpoint: 'ws://127.0.0.1:29999',
            blockHistorySize: 15 * 60 * 2,
            stallCounter: 5,
            readerWorkerAmount: 4,
            evmWorkerAmount: 4,
        },
    },
    target: {
        elastic: {
            node: 'http://127.0.0.1:9200',
            auth: {
                username: 'elastic',
                password: 'password',
            },
            requestTimeout: 5 * 1000,
            docsPerIndex: 10000000,
            scrollSize: 6000,
            scrollWindow: '1m',
            dumpSize: 2000,
            suffix: {
                block: 'block-v1.5',
                transaction: 'transaction-v1.5',
                account: 'account-v1.5',
                accountstate: 'accountstate-v1.5',
                error: 'error-v1.5',
                fork: 'fork-v1.5',
            },
        },
    },
    logLevel: 'debug',
    readerLogLevel: 'info',
    runtime: {},
    broadcast: {
        wsHost: '127.0.0.1',
        wsPort: 7300,
    },
});

export {
    ElasticConnectorConfigSchema,
    ChainConfigSchema,
    BroadcasterConfigSchema,
    ConnectorConfigSchema,
    SourceConnectorConfigSchema,
    TranslatorConfigSchema,
};
