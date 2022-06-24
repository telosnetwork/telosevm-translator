# telosevm elasticsearch indexer

Consume `nodeos` state history endpoint and take and EVM dump.


### build telosevm-es-indexer docker:

    docker build --tag telosevm-indexer:version .

### launch indexer:

    docker run \
        -it \
        --rm \
        --network=host \
        --env "CHAIN_NAME=telos-local-testnet" \
        --env "CHAIN_ID=41" \
        --env "TELOS_ENDPOINT=http://mainnet.telos.net" \
        --env "TELOS_WS_ENDPOINT=ws://api2.hosts.caleos.io:8999" \
        --env "INDEXER_START_BLOCK=180841000" \
        --env "INDEXER_STOP_BLOCK=4294967295" \
        --env "BROADCAST_HOST=127.0.0.1" \
        --env "BROADCAST_PORT=7300" \
        --env "ELASTIC_NODE=http://localhost:9200" \
        --env "ELASTIC_USERNAME=username" \
        --env "ELASTIC_PASSWORD=password" \
        telosevm-indexer:version


### launch elastic:

    docker run \
        -it \
        --rm \
        --network=host \
        --mount type=bind,source="$(pwd)"/elastic,target=/usr/share/elasticsearch/data \
        --env "elastic_username=elastic" \
        --env "elastic_password=password" \
        --env "http.port=10000-10100" \
        --env "transport.port=10100-10200" \
        --env "xpack.security.enabled=false" \
        telosevm-indexer:elastic

### launch kibana:

    docker run \
        -it \
        --rm \
        --network=host \
        --env "ELASTICSEARCH_HOSTS=http://localhost:9200" \
        --env "ELASTICSEARCH_USERNAME=elastic" \
        --env "ELASTICSEARCH_PASSWORD=password" \
        docker.elastic.co/kibana/kibana:7.17.4


### install dependencies

    npm i


### compile and run:

    npx tsc && node build/main.js
