import {APIClient, FetchProvider} from "@wharfkit/antelope";


export function parseAsset(s: string) {
    if (typeof s !== 'string') {
        throw new Error('Expected string containing asset');
    }
    s = s.trim();
    let pos = 0;
    let amount = '';
    let precision = 0;
    if (s[pos] === '-') {
        amount += '-';
        ++pos;
    }
    let foundDigit = false;
    while (pos < s.length && s.charCodeAt(pos) >= '0'.charCodeAt(0) && s.charCodeAt(pos) <= '9'.charCodeAt(0)) {
        foundDigit = true;
        amount += s[pos];
        ++pos;
    }
    if (!foundDigit) {
        throw new Error('Asset must begin with a number');
    }
    if (s[pos] === '.') {
        ++pos;
        while (pos < s.length && s.charCodeAt(pos) >= '0'.charCodeAt(0) && s.charCodeAt(pos) <= '9'.charCodeAt(0)) {
            amount += s[pos];
            ++precision;
            ++pos;
        }
    }
    const name = s.substr(pos).trim();

    return {
        amount: amount,
        precision: precision,
        symbol: name
    };
}

import fetch from 'node-fetch'


export function getRPCClient(endpoint: string) {
    return new APIClient({
       provider: new FetchProvider(endpoint, {fetch})
    });
}
