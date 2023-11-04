import click
from typing import List, Tuple
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

# Assume es is an instance of Elasticsearch client already configured

def check_hashes_around_block(es, index, block_number, range_width=100) -> Tuple[bool, str]:
    # Define the search query
    search_body = {
        'query': {
            'range': {
                'block_num': {
                    'gte': block_number - range_width,
                    'lte': block_number + range_width
                }
            }
        },
        'sort': [
            {'block_num': {'order': 'asc'}}  # Sort by block_num in ascending order
        ],
        'size': range_width * 3
    }

    # Dictionary to store block hashes
    block_hashes = {}

    # Perform the search query
    response = scan(es,
                    index=index,
                    query=search_body)

    # Process the results
    for doc in response:
        source = doc['_source']
        block_num = source['block_num']
        evm_block_hash = source.get('@evmBlockHash')
        prev_evm_block_hash = source.get('@evmPrevBlockHash')

        # Store the current evmBlockHash
        block_hashes[block_num] = evm_block_hash

        # Check if the prevEvmBlockHash matches the evmBlockHash of the previous block
        if prev_evm_block_hash and (block_num - 1) in block_hashes:
            if prev_evm_block_hash != block_hashes[block_num - 1]:
                return False, f'Mismatch at block {block_num}: {prev_evm_block_hash} != {block_hashes[block_num - 1]}'

    return True, 'All hashes follow the rule'


def find_double_forks(es, index_name) -> List[Tuple[str, int]]:
    # Define the scroll time
    scroll = '2m'  # Scroll time

    # Initialize the scroll
    response = es.search(
        index=index_name,
        scroll=scroll,
        size=1000,
        body={
            'query': {'match_all': {}},
            'sort': [{'timestamp': {'order': 'asc'}}]
        }
    )

    # Use a set to track the lastNonForked values
    old_lastNonForked = None

    double_forks = []

    # Start scrolling
    while True:
        # Break out of the loop when there are no more documents
        if not response['hits']['hits']:
            break

        # Process hits
        for doc in response['hits']['hits']:
            # Extract the lastNonForked value
            current_lastNonForked = doc['_source']['lastNonForked']

            # Compare with the previous lastNonForked if it exists
            if old_lastNonForked is not None:
                diff = current_lastNonForked - old_lastNonForked
                if diff < 1000:
                    double_forks.append((doc['_id'], current_lastNonForked))

            # Update the lastNonForked
            old_lastNonForked = current_lastNonForked

        # Get the next batch of documents
        response = es.scroll(scroll_id=response['_scroll_id'], scroll=scroll)

    # Clear the scroll when done
    es.clear_scroll(scroll_id=response['_scroll_id'])

    return double_forks


@click.command()
@click.option('--es-host', default='http://localhost:9200', help='Elasticsearch host.')
@click.option('--fork-index', default='telos-mainnet-fork-*', help='Fork index pattern.')
@click.option('--block-index', default='telos-mainnet-delta-*', help='Block index pattern.')
def main(es_host, fork_index, block_index):
    es = Elasticsearch(es_host)
    for doc_id, block_num in find_double_forks(es, fork_index):
        result = check_hashes_around_block(es, block_index, block_num)
        print(f'Fork doc {doc_id} at block {block_num}: {result}')

if __name__ == '__main__':
    main()
