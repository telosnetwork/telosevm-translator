# telosevm elasticsearch indexer

Consume `nodeos` state history endpoint and take and EVM dump.

## code "map":

- `src/main.ts` - Command line entry point, build main `TEVMIndexer` object 
instance from config file or envoinrment vars.
- `src/indexer.ts` - Main indexer class file, consumes unordered native block +
evm transaction feed and produces an ordered evm block feed with valid hashes.
- `src/ship.ts` - State history connector, opens a websocket client to a nodeos
state history endpoint and deserializes the raw feed, produces unordered native
block feed + evm transactions.
- `src/publisher.ts` - Websocket server used to support eth ws rpc calls like
`newHeads`, once a block is assembled in order and written to db we push it on
a websocket broadcast.
- `src/handlers.ts` - When ship client finds a native transaction that generates
an ethereum transaction it calls one of these "handlers" can be one of three.
- `database` - Elasticsearch connector with all custom queries we use, like
fast gap checking.
- `workers` - Implementations of workers used to paralelize deserialization.
- `utils` - All kinds of evm, eosio, ship, logging utils.
- `abis` - Standard native abis used to deserialize incoming `get_block`
responses.

## install & run:

### launch elastic:

    docker run \
        -itd \
        --rm \
        --network=host \
        --mount type=bind,source="$(pwd)"/elastic,target=/usr/share/elasticsearch/data \
        --env "elastic_username=elastic" \
        --env "elastic_password=password" \
        --env "xpack.security.enabled=false" \
        telosevm-indexer:elastic

### launch indexer (manually):

### requirements:

    node 16

#### install dependencies

    yarn install

#### compile and run:

    npx tsc && node build/main.js

### launch indexer (through docker):

#### build telosevm-es-indexer docker:

    docker build --tag telosevm-indexer:$VERSION .

    docker run \
        -it \
        --rm \
        --network=host \
        --env "CHAIN_NAME=telos-testnet" \
        --env "CHAIN_ID=41" \
        --env "TELOS_ENDPOINT=http://mainnet.telos.net" \
        --env "TELOS_WS_ENDPOINT=ws://api2.hosts.caleos.io:8999" \
        --env "EVM_DEPLOY_BLOCK=180841000" \
        --env "EVM_DELTA=57" \
        --env "INDEXER_START_BLOCK=180841000" \
        --env "INDEXER_STOP_BLOCK=4294967295" \
        --env "BROADCAST_HOST=127.0.0.1" \
        --env "BROADCAST_PORT=7300" \
        --env "ELASTIC_NODE=http://localhost:9200" \
        --env "ELASTIC_USERNAME=username" \
        --env "ELASTIC_PASSWORD=password" \
        telosevm-indexer:$VERSION

### launch kibana (OPTIONAL):

    docker run \
        -itd \
        --rm \
        --network=host \
        --env "ELASTICSEARCH_HOSTS=http://localhost:9200" \
        --env "ELASTICSEARCH_USERNAME=elastic" \
        --env "ELASTICSEARCH_PASSWORD=password" \
        docker.elastic.co/kibana/kibana:7.17.4

