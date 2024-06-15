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

const HashSchema = z.string().transform((val) => {
    const stripped = val.startsWith('0x') ? val.slice(2) : val;
    return stripped;
});

const ChainConfigSchema = z.object({
    chainName: z.string(),
    chainId: z.number(),
    startBlock: BigIntSchema,
    stopBlock: BigIntSchema.default(-1n),
    evmBlockDelta: BigIntSchema,
    evmPrevHash: HashSchema.default(''),
    evmValidateHash: HashSchema.default(''),
    irreversibleOnly: z.boolean().default(false),
});

const ConnectorConfigSchema = z.object({
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
        maxWsPayloadMb: z.number().default(2048),
        fetchDeltas: z.boolean().default(true),
        fetchTraces: z.boolean().default(true)
    }),
    arrow: ArrowBatchConfigSchema,
    logLevel: z.string().optional(),
    trimFrom: z.number().optional(),
    skipIntegrityCheck: z.boolean().optional(),
    gapsPurge: z.boolean().optional(),
});

const TranslatorConfigSchema = z.object({
    connector: ConnectorConfigSchema,
    logLevel: z.string(),
    readerLogLevel: z.string(),
    runtime: z.object({
        eval: z.boolean().optional(),
        timeout: z.number().optional(),
        onlyDBCheck: z.boolean().optional(),
    })
});

export type ArrowConnectorConfig = z.infer<typeof ArrowBatchConfigSchema>;
export type ChainConfig = z.infer<typeof ChainConfigSchema>;
export type ConnectorConfig = z.infer<typeof ConnectorConfigSchema>;
export type TranslatorConfig = z.infer<typeof TranslatorConfigSchema>;

export const DEFAULT_CONF = TranslatorConfigSchema.parse({
    connector: {
        chain: {
            chainName: 'telos-local',
            chainId: 41,
            startBlock: '35',
            evmBlockDelta: 2,
            evmPrevHash: '',
            evmValidateHash: '',
            irreversibleOnly: false,
        },
        arrow: {
            dataDir: 'arrow-data'
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
    logLevel: 'debug',
    readerLogLevel: 'info',
    runtime: {},
});

export {
    ChainConfigSchema,
    ConnectorConfigSchema,
    TranslatorConfigSchema,
};
