import {CompatTarget} from "./types/config.js";
import semver from 'semver';

export interface CompatRange {
    from: CompatTarget
    to?: CompatTarget
}

function compatTargetStr(target: CompatTarget) : string {
    return `${target.mayor}.${target.minor}.${target.patch}`;
}

export class FeatureManager {

    readonly targetCompat: CompatTarget

    readonly features: Map<string, CompatRange>;

    constructor(target: CompatTarget, features: {[key: string]: CompatRange}) {
        this.targetCompat = target;

        this.features = new Map<string, CompatRange>(Object.entries(features));
    }

    isFeatureEnabled(name: string) : boolean {
        const range = this.features.get(name);

        if (semver.lt(compatTargetStr(this.targetCompat), compatTargetStr(range.from)))
            return false;

        if (range.to && semver.gte(compatTargetStr(this.targetCompat), compatTargetStr(range.to)))
            return false;

        return true;
    }
}


export let featureManager = undefined;

export function initFeatureManager(target: CompatTarget) {
    featureManager = new FeatureManager(
        target,
        {
            'STORE_ITXS': {
                from: {
                    mayor: 1, minor: 0, patch: 0
                },
                to: {
                    mayor: 2, minor: 0, patch: 0
                }
            },
            'STORE_ACC_DELTAS': {
                from: {
                    mayor: 2, minor: 0, patch: 0
                }
            }
        }
    );
}