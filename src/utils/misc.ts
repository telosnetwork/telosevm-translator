export function portFromEndpoint(endpoint: string): number {
    return parseInt(endpoint.split(':')[2]);
}