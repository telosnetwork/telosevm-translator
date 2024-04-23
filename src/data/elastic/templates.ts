export function getTemplatesForChain(
    chain: string,
    suffixConfig: {[key: string]: string},
    numberOfShards: number,
    numberOfReplicas: number,
    refreshInterval: number,
    codec: string
) {
    const indexSettings = {
        number_of_shards: numberOfShards,
        number_of_replicas: numberOfReplicas,
        refresh_interval: refreshInterval,
        codec
    };

    const transaction = {
        index_patterns: [`${chain}-${suffixConfig.transaction}-*`],
        settings: {
            index: {
                ...indexSettings,
                sort: {
                    field: ['@raw.block', '@raw.trx_index'],
                    order: ['desc', 'desc']
                }
            }
        },
        mappings: {
            properties: {
                '@timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_millis'},
                'trx_id': {'type': 'keyword'},
                'action_ordinal': {'type': 'long'},
                'signatures': {'enabled': false},

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
                        "raw": {"type": "binary"},
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
                        'output': {'enabled': false},
                        'logsBloom': {'type': 'text'},
                        'errors': {'enabled': false},
                    }
                },
            }
        }
    };

    const delta = {
        index_patterns: [`${chain}-${suffixConfig.delta}-*`],
        settings: {
            index: {
                ...indexSettings,
                sort: {
                    field: 'block_num',
                    order: 'desc'
                }
            }
        },
        mappings: {
            properties: {
                '@timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_millis'},
                'block_num': {'type': 'long'},
                '@global': {
                    'properties': {
                        'block_num': {'type': 'long'}
                    }
                },

                '@blockHash': {'type': 'keyword'},
                '@evmBlockHash': {'type': 'keyword'},
                '@evmPrevBlockHash': {'type': 'keyword'},
                '@receiptsRootHash': {'type': 'keyword'},
                '@transactionsRoot': {'type': 'keyword'},

                'gasUsed': {'type': 'long'},
                'gasLimit': {'type': 'long'},
                'size': {'type': 'text'},
                'txAmount': {'type': 'long'}
            }
        }
    };

    const error = {
        index_patterns: [`${chain}-${suffixConfig.error}-*`],
        settings: {
            index: {
                ...indexSettings
            }
        },
        mappings: {
            properties: {
                'timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_millis'},
                'info': {'enabled': true},
                'name': {'type': 'keyword'},
                'stack': {'type': 'keyword'},
                'message': {'type': 'keyword'},
            }
        }
    };

    const fork = {
        index_patterns: [`${chain}-${suffixConfig.fork}-*`],
        settings: {
            index: {
                ...indexSettings
            }
        },
        mappings: {
            properties: {
                'timestamp': {'type': 'date'},
                'lastNonForked': {'type': 'long'},
                'lastForked': {'type': 'long'},
            }
        }
    };

    const accountDelta = {
        index_patterns: [`${chain}-${suffixConfig.account}-*`],
        settings: {
            index: {
                ...indexSettings
            }
        },
        mappings: {
            properties: {
                "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "block_num": {"type": "long"},
                "ordinal": {"type": "long"},
                "index": {"type": "long"},
                "address": {"type": "keyword"},
                "account": {"type": "keyword"},
                "nonce": {"type": "long"},
                "code": {"enabled": false},
                "balance": {"type": "keyword"},
            }
        }
    };

    const accountStateDelta = {
        index_patterns: [`${chain}-${suffixConfig.accountstate}-*`],
        settings: {
            index: {
                ...indexSettings
            }
        },
        mappings: {
            properties: {
                "timestamp": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                "block_num": {"type": "long"},
                "ordinal": {"type": "long"},
                "index": {"type": "long"},
                "key": {"type": "keyword"},
                "value": {"type": "keyword"},
            }
        }
    };

    return {
        transaction, delta, error, fork,
        account: accountDelta,
        accountstate: accountStateDelta
    }
}
