FROM node:16-bullseye

RUN mkdir -p /indexer/build

COPY src/ /indexer/src 
COPY package.json /indexer
COPY tsconfig.json /indexer
COPY config.json /indexer

WORKDIR /indexer

RUN yarn install
RUN npx tsc

CMD ["node", "build/main.js"]
