import {StorageEosioActionSchema, StorageEosioDeltaSchema} from "../../types/evm.js";
import {sampleActionDocument, sampleDeltaDocument} from "../samples.js";

import {assert} from "chai";

describe('Elastic Document Validation', function() {
    it('document samples', function () {
        const actionResult = StorageEosioActionSchema.safeParse(sampleActionDocument);
        const deltaResult = StorageEosioDeltaSchema.safeParse(sampleDeltaDocument);
        // @ts-ignore
        assert(actionResult.success, `Mainnet sample action not parseable!:\n${JSON.stringify(actionResult.error, null, 4)}`);
        // @ts-ignore
        assert(deltaResult.success, `Mainnet sample delta not parseable!:\n${JSON.stringify(deltaResult.error, null, 4)}`);
    });
});