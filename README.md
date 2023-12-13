# telosevm-translator

Consume `nodeos` state history endpoint and take an EVM dump.

## install & run:

### using docker:

    // launch dependencies:
    npm run elastic-start
    // optional:
    npm run kibana-start

    // build & launch translator:
    npm run docker-start

### launch indexer (manually):

#### install dependencies & compile:
    
    npm run build

#### run:

    npm run start

#### to run via PM2, give it a unique name that makes sense (first install PM2 if not already installed):

    npm run build && pm2 start build/main.js --name telostest-evm15-translator