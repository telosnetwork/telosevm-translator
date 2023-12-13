from node:18-bullseye

run mkdir -p /indexer/build

copy src/ /indexer/src 
copy package.json /indexer
copy tsconfig.json /indexer
copy config.json /indexer
copy eosrio-hyperion-sequential-reader-1.2.5.tgz /indexer

workdir /indexer

run npm run build

cmd ["npm", "run", "start"]
