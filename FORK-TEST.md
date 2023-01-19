# Testing Procedure for Telos Blockchain Indexer

## Introduction

This testing procedure is designed to verify the ability of the Telos blockchain indexer to handle fork events. The indexer reads blocks in order from a node state history websocket endpoint and processes them to generate an EVM-compatible stream of blocks inside an elastic database. This means reading transactions as they happen on the native Telos chain and translating them to EVM-compatible transactions. 

On Telos blockchain, a fork can occur due to many factors, such as the node we are reading from having a divergent chain. This usually corrects itself quickly after consensus is re-established with the other connected nodes. The goal of this testing procedure is to simulate fork events and verify that the indexer can detect and handle them correctly.

## Test Setup

1. Set up a private stagenet chain for Telos using the Telos blockchain software with a multi-producer setup and staked votes.
2. Configure one of the nodes with the following command line parameters: `--last-block-time-offset=0 --last-block-cpu-effort-percent=100`
3. Configure and launch the indexer and wait for it to be synced to head

## Test Steps

1. Create a series of transactions on the private chain.
2. Observe the behavior of the indexer and the integrity of the elastic database.
3. Check that the indexer can detect and handle the fork correctly.
