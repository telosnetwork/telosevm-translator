import click
from typing import List, Tuple
from elasticsearch import Elasticsearch

from utils import check_hashes_around_block


def verify_forks(es, block_index, fork_index) -> List[Tuple[str, int]]:
    # Define the scroll time
    scroll = '2m'  # Scroll time

    # Initialize the scroll
    response = es.search(
        index=fork_index,
        scroll=scroll,
        size=1000,
        body={
            'query': {'match_all': {}},
            'sort': [{'timestamp': {'order': 'asc'}}]
        }
    )

    # Start scrolling
    while True:
        # Break out of the loop when there are no more documents
        if not response['hits']['hits']:
            break

        # Process hits
        for doc in response['hits']['hits']:
            block_num = doc['_source']['lastNonForked']
            ok, msg = check_hashes_around_block(es, block_index, block_num)
            if not ok:
                print(msg)
        # Get the next batch of documents
        response = es.scroll(scroll_id=response['_scroll_id'], scroll=scroll)

    # Clear the scroll when done
    es.clear_scroll(scroll_id=response['_scroll_id'])


@click.command()
@click.option('--es-host', default='http://localhost:9200', help='Elasticsearch host.')
@click.option('--fork-index', default='telos-mainnet-fork-*', help='Fork index pattern.')
@click.option('--block-index', default='telos-mainnet-delta-*', help='Block index pattern.')
def main(es_host, fork_index, block_index):
    es = Elasticsearch(es_host)
    verify_forks(es, block_index, fork_index)

if __name__ == '__main__':
    main()
