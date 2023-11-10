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
