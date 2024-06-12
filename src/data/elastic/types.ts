import {isValidAddress} from "@ethereumjs/util";
import {Asset} from "@wharfkit/antelope";
import {
    isInteger,
    isValidAntelopeHash,
    isValidEVMHash,
    isValidHexString,
    isValidUnprefixedEVMAddress,
    isValidUnprefixedEVMHash,
    isValidUnprefixedHexString,
} from "../../utils/validation.js";
import { z } from 'zod';

export const InternalEvmTransactionSchema = z.object({
    callType: z.string(),
    from: z.string(),
    gas: z.string(),
    input: z.string(),
    input_trimmed: z.string(),
    to: z.string(),
    value: z.string(),
    gasUsed: z.string(),
    output: z.string(),
    subtraces: z.string(),
    traceAddress: z.array(z.number()),
    type: z.string(),
    depth: z.string(),
    extra: z.any()
});

export type InternalEvmTransaction = z.infer<typeof InternalEvmTransactionSchema>;

export const StorageEvmTransactionSchema = z.object({
    hash: z.string().refine(obj => isValidEVMHash(obj), { message: "Invalid EVM hash" }),
    from: z.string().optional().refine(obj => isValidAddress(obj) || obj === undefined, { message: "Invalid address" }),
    trx_index: z.number(),
    block: z.number(),
    block_hash: z.string().refine(obj => isValidUnprefixedEVMHash(obj), { message: "Invalid unprefixed EVM hash" }),
    to: z.string().optional().refine(obj => isValidAddress(obj) || obj === undefined, { message: "Invalid address" }),
    input_data: z.string().refine(obj => isValidHexString(obj) || obj === '', { message: "Invalid hex string" }),
    input_trimmed: z.string().refine(obj => isValidHexString(obj) || obj === '', { message: "Invalid hex string" }),
    value: z.string().refine(isValidUnprefixedHexString, { message: "Invalid unprefixed hex string" }),
    value_d: z.string().refine(obj => {
        try {
            Asset.fromString(obj);
            return true;
        } catch {
            return isInteger(obj);
        }
    }, { message: "Invalid asset string" }),
    nonce: z.string().refine(isInteger, { message: "Invalid integer" }),
    gas_price: z.string().refine(isInteger, { message: "Invalid integer" }),
    gas_limit: z.string().refine(isInteger, { message: "Invalid integer" }),
    status: z.number(),
    itxs: z.array(InternalEvmTransactionSchema),
    epoch: z.number(),
    createdaddr: z.string().refine(obj => isValidUnprefixedEVMAddress(obj) || obj === '', { message: "Invalid created address" }),
    gasused: z.string().refine(isInteger, { message: "Invalid integer" }),
    gasusedblock: z.string().refine(isInteger, { message: "Invalid integer" }),
    charged_gas_price: z.string().refine(isInteger, { message: "Invalid integer" }),
    output: z.string(),
    logs: z.array(z.object({
        address: z.string(),
        topics: z.array(z.string()),
        data: z.string()
    })).optional(),
    logsBloom: z.string().optional().refine(obj => isValidUnprefixedHexString(obj) || obj === undefined, { message: "Invalid unprefixed hex string" }),
    errors: z.array(z.string()).optional(),
    raw: z.string(),
    v: z.string().refine(isInteger, { message: "Invalid integer" }),
    r: z.string().refine(isValidHexString, { message: "Invalid hex string" }),
    s: z.string().refine(isValidHexString, { message: "Invalid hex string" }),
});

export type StorageEvmTransaction = z.infer<typeof StorageEvmTransactionSchema>;

export const StorageEosioActionSchema = z.object({
    "@timestamp": z.string().refine((ts) => !isNaN(Date.parse(ts)), {
        message: "Invalid timestamp format",
    }),
    trx_id: z.string().refine(isValidAntelopeHash, {
        message: "Invalid trx_id format",
    }),
    action_ordinal: z.number(),
    signatures: z.array(z.string()).optional(),
    "@raw": StorageEvmTransactionSchema,
});

export type StorageEosioAction = z.infer<typeof StorageEosioActionSchema>;

export const StorageEosioDeltaSchema = z.object({
    "@timestamp": z.string().refine((ts) => !isNaN(Date.parse(ts)), {
        message: "Invalid timestamp format",
    }),
    block_num: z.number(),
    "@global": z.object({
        block_num: z.number(),
    }),
    "@blockHash": z.string().refine(isValidAntelopeHash, {
        message: "Invalid block hash format",
    }),
    "@evmBlockHash": z.string().refine(isValidUnprefixedEVMHash, {
        message: "Invalid EVM block hash format",
    }),
    "@evmPrevBlockHash": z.string().refine(isValidUnprefixedEVMHash, {
        message: "Invalid EVM previous block hash format",
    }),
    "@receiptsRootHash": z.string().refine(isValidUnprefixedEVMHash, {
        message: "Invalid receipts root hash format",
    }),
    "@transactionsRoot": z.string().refine(isValidUnprefixedEVMHash, {
        message: "Invalid transactions root format",
    }),
    gasUsed: z.string().refine(isInteger, {
        message: "Invalid gasUsed format",
    }),
    gasLimit: z.string().refine(isInteger, {
        message: "Invalid gasLimit format",
    }),
    txAmount: z.number().default(0),
    size: z.string().refine(isInteger, {
        message: "Invalid size format",
    }),
});

export type StorageEosioDelta = z.infer<typeof StorageEosioDeltaSchema>;

export const StorageAccountDeltaSchema = z.object({
    timestamp: z.string().refine((ts) => !isNaN(Date.parse(ts)), {
        message: "Invalid timestamp format",
    }),
    block_num: z.number(),
    ordinal: z.number(),
    index: z.number(),
    address: z.string(),
    account: z.string(),
    nonce: z.number(),
    code: z.string(),
    balance: z.string(),
});

export type StorageAccountDelta = z.infer<typeof StorageAccountDeltaSchema>;

export const StorageAccountStateDeltaSchema = z.object({
    timestamp: z.string().refine((ts) => !isNaN(Date.parse(ts)), {
        message: "Invalid timestamp format",
    }),
    block_num: z.number(),
    ordinal: z.number(),
    scope: z.string(),
    index: z.number(),
    key: z.string(),
    value: z.string(),
});

export type StorageAccountStateDelta = z.infer<typeof StorageAccountStateDeltaSchema>;

export function isStorableDocument(obj: any): boolean {
    // use genesis schema for delta docs as its more permissive
    const isDelta = StorageEosioDeltaSchema.safeParse(obj).success;
    const isAction = StorageEosioActionSchema.safeParse(obj).success;
    const isAccDelta = StorageAccountDeltaSchema.safeParse(obj).success;
    const isAccStateDelta = StorageAccountStateDeltaSchema.safeParse(obj).success;
    return isAction || isDelta || isAccDelta || isAccStateDelta;
}
