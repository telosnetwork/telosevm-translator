export function getTemplatesForChain(chain: string) {
    const shards = 2;
    const replicas = 0;
    const refresh = "1s";

    // LZ4 Compression
    // const compression = 'default';
    // DEFLATE
    const compression = "best_compression";

    const actionSettings = {
        index: {
            codec: compression,
            refresh_interval: refresh,
            number_of_shards: shards * 2,
            number_of_replicas: replicas,
            sort: {
                field: ["@raw.block", "action_ordinal"],
                order: ["asc", "asc"]
            }
        }
    };

    const action = {
        order: 0,
        index_patterns: [
            chain + "-action-*"
        ],
        settings: actionSettings,
        mappings: {
            properties: {
                "@timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "action_ordinal": {"type": "long"},
                "trx_id": {"type": "keyword"},

                "@raw": {
                    "properties": {
                        "hash": {"type": "keyword"},
                        "trx_index": {"type": "long"},
                        "block": {"type": "long"},
                        "block_hash": {"type": "keyword"},
                        "from": {"type": "keyword"},
                        "to": {"type": "keyword"},
                        "input_data": {"enabled": "false"},
                        "input_trimmed": {"type": "keyword"},
                        "value": {"type": "text"},
                        "value_d": {"type": "text"},
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
            }
        }
    };

    const deltaSettings = {
        "index": {
            "codec": compression,
            "number_of_shards": shards * 2,
            "refresh_interval": refresh,
            "number_of_replicas": replicas,
            "sort.field": ["block_num"],
            "sort.order": ["asc"]
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
                "@timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "block_num": {"type": "long"},

                "@global": {
                    "properties": {
                        "block_num": {"type": "long"}
                    }
                },

                // hashes
                "@evmPrevBlockHash": {"type": "keyword"},
                "@evmBlockHash": {"type": "keyword"},
                "@blockHash": {"type": "keyword"},
                "@receiptsRootHash": {"type": "keyword"},
                "@transactionsRoot": {"type": "keyword"},

                "gasUsed": {"type": "long"},
                "gasLimit": {"type": "long"}
            }
        }
    };

    const errorSettings = {
        "index": {
            "codec": compression,
            "number_of_shards": shards * 2,
            "refresh_interval": refresh,
            "number_of_replicas": replicas,
            "sort.field": ["timestamp"],
            "sort.order": ["desc"]
        }
    };

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

    const forkSettings = {
        "index": {
            "codec": compression,
            "number_of_shards": shards * 2,
            "refresh_interval": refresh,
            "number_of_replicas": replicas,
            "sort.field": ["timestamp"],
            "sort.order": ["desc"]
        }
    };

    const fork = {
        "index_patterns": [chain + "-fork-*"],
        "settings": forkSettings,
        "mappings": {
            "properties": {

                // base fields
                "timestamp": {"type": "date"},
                "lastNonForked": {"type": "long"},
                "lastForked": {"type": "long"},
            }
        }
    };

    return {
        action, delta, error, fork
    }
}
