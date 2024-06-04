import semver from 'semver';

export interface CompatRange {
    from: string
    to?: string
}

export class FeatureManager {

    readonly targetCompat: string

    readonly features: Map<string, CompatRange>;

    constructor(target: string, features: {[key: string]: CompatRange}) {
        this.targetCompat = target;

        this.features = new Map<string, CompatRange>(Object.entries(features));
    }

    isFeatureEnabled(name: string) : boolean {
        const range = this.features.get(name);

        if (semver.lt(this.targetCompat, range.from))
            return false;

        if (range.to && semver.gte(this.targetCompat, range.to))
            return false;

        return true;
    }
}


export let featureManager = undefined;

export function initFeatureManager(target: string) {
    featureManager = new FeatureManager(
        target,
        {
            'STORE_ITXS': {
                from: '1.0.0',
                to: '2.0.0'
            },
            'STORE_ACC_DELTAS': {
                from: '2.0.0'
            }
        }
    );
}