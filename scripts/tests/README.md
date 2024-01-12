### elasticdump creation procedure:
In kibana dev tools on the instance that contains the source data:

sources:
- `telos-mainnet-tevmc-action-v1.5-00000031`
- `telos-mainnet-tevmc-delta-v1.5-00000031`

target:
- `telos-mainnet-verification-action-v1.5-00000031`
- `telos-mainnet-verification-delta-v1.5-00000031`

```
PUT /telos-mainnet-tevmc-delta-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": true
  }
}

PUT /telos-mainnet-tevmc-action-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": true
  }
}


POST /telos-mainnet-tevmc-delta-v1.5-00000031/_clone/telos-mainnet-verification-delta-v1.5-00000031
POST /telos-mainnet-tevmc-action-v1.5-00000031/_clone/telos-mainnet-verification-action-v1.5-00000031

PUT /telos-mainnet-verification-delta-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": false
  }
}

PUT /telos-mainnet-verification-action-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": false
  }
}

POST /telos-mainnet-verification-delta-v1.5-00000031/_delete_by_query
{
  "query": {
    "range": {
      "@global.block_num": {
        "gt": 312415000
      }
    }
  }
}

POST /telos-mainnet-verification-action-v1.5-*/_delete_by_query
{
  "query": {
    "range": {
      "@raw.block": {
        "gt": 312415000
      }
    }
  }
}

POST /telos-mainnet-verification-delta-v1.5-00000031/_delete_by_query
{
  "query": {
    "range": {
      "@global.block_num": {
        "lt": 312315000
      }
    }
  }
}

POST /telos-mainnet-verification-action-v1.5-*/_delete_by_query
{
  "query": {
    "range": {
      "@raw.block": {
        "lt": 312315000
      }
    }
  }
}

PUT /telos-mainnet-verification-delta-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": true
  }
}

PUT /telos-mainnet-verification-action-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": true
  }
}

PUT /telos-mainnet-tevmc-delta-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": false
  }
}

PUT /telos-mainnet-tevmc-action-v1.5-00000031/_settings
{
  "settings": {
    "index.blocks.write": false
  }
}
```