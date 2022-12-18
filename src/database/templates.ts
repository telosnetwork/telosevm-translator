export function getTemplatesForChain(chain: string) {
    const shards = 2;
    const replicas = 0;
    const refresh = "1s";
    let defaultLifecyclePolicy = "200G";

    // LZ4 Compression
    // const compression = 'default';
    // DEFLATE
    const compression = "best_compression";

    const defaultIndexSettings = {
        "index": {
            "number_of_shards": shards,
            "refresh_interval": refresh,
            "number_of_replicas": replicas,
            "codec": compression
        }
    };

    const actionSettings = {
        index: {
            lifecycle: {
                "name": defaultLifecyclePolicy,
                "rollover_alias": chain + "-action"
            },
            codec: compression,
            refresh_interval: refresh,
            number_of_shards: shards * 2,
            number_of_replicas: replicas,
            sort: {
                field: "global_sequence",
                order: "desc"
            }
        }
    };

    // if (cm.config.settings.hot_warm_policy) {
    //     actionSettings["routing"] = {"allocation": {"exclude": {"data": "warm"}}};
    // }

    const action = {
        order: 0,
        index_patterns: [
            chain + "-action-*"
        ],
        settings: actionSettings,
        mappings: {
            properties: {
                "@timestamp": {"type": "date"},
                "ds_error": {"type": "boolean"},
                "global_sequence": {"type": "long"},
                "account_ram_deltas.delta": {"type": "integer"},
                "account_ram_deltas.account": {"type": "keyword"},
                "act.authorization.permission": {"enabled": false},
                "act.authorization.actor": {"type": "keyword"},
                "act.account": {"type": "keyword"},
                "act.name": {"type": "keyword"},
                "act.data": {"enabled": false},
                "block_num": {"type": "long"},
                "action_ordinal": {"type": "long"},
                "creator_action_ordinal": {"type": "long"},
                "cpu_usage_us": {"type": "integer"},
                "net_usage_words": {"type": "integer"},
                "code_sequence": {"type": "integer"},
                "abi_sequence": {"type": "integer"},
                "trx_id": {"type": "keyword"},
                "producer": {"type": "keyword"},
                "notified": {"type": "keyword"},
                "signatures": {"enabled": false},
                "inline_count": {"type": "short"},
                "max_inline": {"type": "short"},
                "inline_filtered": {"type": "boolean"},
                "receipts": {
                    "properties": {
                        "global_sequence": {"type": "long"},
                        "recv_sequence": {"type": "long"},
                        "receiver": {"type": "keyword"},
                        "auth_sequence": {
                            "properties": {
                                "account": {"type": "keyword"},
                                "sequence": {"type": "long"}
                            }
                        }
                    }
                },

                "@raw": {
                    "properties": {
                        "hash": {"type": "text"},
                        "trx_index": {"type": "long"},
                        "block": {"type": "long"},
                        "block_hash": {"type": "text"},
                        "from": {"type": "keyword"},
                        "to": {"type": "keyword"},
                        "input_data": {"enabled": "false"},
                        "input_trimmed": {"type": "keyword"},
                        "value": {"type": "text"},
                        "value_d": {"type": "double"},
                        "nonce": {"type": "long"},
                        "v": {"enabled": false},
                        "r": {"enabled": false},
                        "s": {"enabled": false},
                        "gas_price": {"type": "double"},
                        "gas_limit": {"type": "double"},
                        "status": {"type": "byte"},
                        "epoch": {"type": "long"},
                        "createdaddr": {"type": "keyword"},
                        // TODO: Long vs Double on the gasprice/limit/used
                        "charged_gas_price": {"type": "double"},
                        "gasused": {"type": "long"},
                        "gasusedblock": {"type": "long"},
                        "logs": {
                            "properties": {
                                "address": {"type": "keyword"},
                                "data": {"enabled": false},
                                "topics": {"type": "keyword"}
                            }
                        },
                        "logsBloom": {"type": "text"},
                        "output": {"enabled": false},
                        "errors": {"enabled": false},
                        "itxs": {
                            "properties": {
                                "callType": {"type": "text"},
                                "from": {"type": "keyword"},
                                "gas": {"enabled": false},
                                "input": {"enabled": false},
                                "input_trimmed": {"type": "keyword"},
                                "to": {"type": "keyword"},
                                "value": {"type": "text"},
                                "gasUsed": {"enabled": false},
                                "output": {"enabled": false},
                                "subtraces": {"type": "long"},
                                "traceAddress": {"enabled": false},
                                "type": {"type": "text"},
                                "depth": {"enabled": false},
                                "extra": {"type": "object", "enabled": false}
                            }
                        },
                    }
                },

                // eosio::newaccount
                "@newaccount": {
                    "properties": {
                        "active": {"type": "object"},
                        "owner": {"type": "object"},
                        "newact": {"type": "keyword"}
                    }
                },

                // eosio::updateauth
                "@updateauth": {
                    "properties": {
                        "permission": {"type": "keyword"},
                        "parent": {"type": "keyword"},
                        "auth": {"type": "object"}
                    }
                },

                // *::transfer
                "@transfer": {
                    "properties": {
                        "from": {"type": "keyword"},
                        "to": {"type": "keyword"},
                        "amount": {"type": "float"},
                        "symbol": {"type": "keyword"},
                        "memo": {"type": "text"}
                    }
                },

                // eosio::unstaketorex
                "@unstaketorex": {
                    "properties": {
                        "owner": {"type": "keyword"},
                        "receiver": {"type": "keyword"},
                        "amount": {"type": "float"}
                    }
                },

                // eosio::buyrex
                "@buyrex": {
                    "properties": {
                        "from": {"type": "keyword"},
                        "amount": {"type": "float"}
                    }
                },

                // eosio::buyram
                "@buyram": {
                    "properties": {
                        "payer": {"type": "keyword"},
                        "receiver": {"type": "keyword"},
                        "quant": {"type": "float"}
                    }
                },

                // eosio::buyrambytes
                "@buyrambytes": {
                    "properties": {
                        "payer": {"type": "keyword"},
                        "receiver": {"type": "keyword"},
                        "bytes": {"type": "long"}
                    }
                },

                // eosio::delegatebw
                "@delegatebw": {
                    "properties": {
                        "from": {"type": "keyword"},
                        "receiver": {"type": "keyword"},
                        "stake_cpu_quantity": {"type": "float"},
                        "stake_net_quantity": {"type": "float"},
                        "transfer": {"type": "boolean"},
                        "amount": {"type": "float"}
                    }
                },

                // eosio::undelegatebw
                "@undelegatebw": {
                    "properties": {
                        "from": {"type": "keyword"},
                        "receiver": {"type": "keyword"},
                        "unstake_cpu_quantity": {"type": "float"},
                        "unstake_net_quantity": {"type": "float"},
                        "amount": {"type": "float"}
                    }
                }
            }
        }
    };

    const deltaSettings = {
        "index": {
            "lifecycle": {
                "name": defaultLifecyclePolicy,
                "rollover_alias": chain + "-delta"
            },
            "codec": compression,
            "number_of_shards": shards * 2,
            "refresh_interval": refresh,
            "number_of_replicas": replicas,
            "sort.field": ["block_num", "scope", "primary_key"],
            "sort.order": ["desc", "asc", "asc"]
        }
    };

    // if (cm.config.settings.hot_warm_policy) {
    //     deltaSettings["routing"] = {"allocation": {"exclude": {"data": "warm"}}};
    // }

    const delta = {
        "index_patterns": [chain + "-delta-*"],
        "settings": deltaSettings,
        "mappings": {
            "properties": {

                // base fields
                "@timestamp": {"type": "date"},
                "ds_error": {"type": "boolean"},
                "block_id": {"type": "keyword"},
                "block_num": {"type": "long"},
                "deleted_at": {"type": "long"},
                "code": {"type": "keyword"},
                "scope": {"type": "keyword"},
                "table": {"type": "keyword"},
                "payer": {"type": "keyword"},
                "primary_key": {"type": "keyword"},
                "data": {"enabled": false},
                "value": {"enabled": false},

                // eosio.msig::approvals
                "@approvals.proposal_name": {"type": "keyword"},
                "@approvals.provided_approvals": {"type": "object"},
                "@approvals.requested_approvals": {"type": "object"},

                // eosio.msig::proposal
                "@proposal.proposal_name": {"type": "keyword"},
                "@proposal.transaction": {"enabled": false},

                // *::accounts
                "@accounts.amount": {"type": "float"},
                "@accounts.symbol": {"type": "keyword"},

                // eosio::voters
                "@voters.is_proxy": {"type": "boolean"},
                "@voters.producers": {"type": "keyword"},
                "@voters.last_vote_weight": {"type": "double"},
                "@voters.proxied_vote_weight": {"type": "double"},
                "@voters.staked": {"type": "float"},
                "@voters.proxy": {"type": "keyword"},

                // eosio::producers
                "@producers.total_votes": {"type": "double"},
                "@producers.is_active": {"type": "boolean"},
                "@producers.unpaid_blocks": {"type": "long"},

                // eosio::global
                "@global": {
                    "properties": {
                        "last_name_close": {"type": "date"},
                        "last_pervote_bucket_fill": {"type": "date"},
                        "last_producer_schedule_update": {"type": "date"},
                        "perblock_bucket": {"type": "double"},
                        "pervote_bucket": {"type": "double"},
                        "total_activated_stake": {"type": "double"},
                        "total_voteshare_change_rate": {"type": "double"},
                        "total_unpaid_voteshare": {"type": "double"},
                        "total_producer_vote_weight": {"type": "double"},
                        "total_ram_bytes_reserved": {"type": "long"},
                        "total_ram_stake": {"type": "long"},
                        "total_unpaid_blocks": {"type": "long"},
                        "block_num": {"type": "long"}
                    }
                },

                // hashes

                "@evmPrevBlockHash": {"type": "keyword"},
                "@evmBlockHash": {"type": "keyword"},
                "@receiptsRootHash": {"type": "keyword"},
                "@transactionsRoot": {"type": "keyword"},

                "gasUsed": {"type": "long"},
                "gasLimit": {"type": "long"}
            }
        }
    };

    const errorSettings = {
        "index": {
            "lifecycle": {
                "name": defaultLifecyclePolicy,
                "rollover_alias": chain + "-error"
            },
            "codec": compression,
            "number_of_shards": shards * 2,
            "refresh_interval": refresh,
            "number_of_replicas": replicas,
            "sort.field": ["timestamp"],
            "sort.order": ["desc"]
        }
    };

    // if (cm.config.settings.hot_warm_policy) {
    //     deltaSettings["routing"] = {"allocation": {"exclude": {"data": "warm"}}};
    // }

    const error = {
        "index_patterns": [chain + "-error-*"],
        "settings": errorSettings,
        "mappings": {
            "properties": {

                // base fields
                "info": {"enabled": true},
                "timestamp": {"type": "date"},
                "name": {"type": "keyword"},
                "stack": {"type": "keyword"},
                "message": {"type": "keyword"},
            }
        }
    };
    return {
        action: action, delta: delta, error: error
    }
}
