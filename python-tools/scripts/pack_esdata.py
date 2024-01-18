import os
import re
import pdbp
import json
import subprocess
import tarfile
import zstandard as zstd
import click
from elasticsearch import Elasticsearch
import tempfile

@click.command()
@click.option('--old-prefix', required=True, help='Old prefix of the Elasticsearch indexes.')
@click.option('--new-prefix', required=True, help='New prefix to replace the old one.')
@click.option('--es-host', required=True, help='Elasticsearch host URL.')
@click.option('--from-block', 'from_block', required=True, type=int, help='Start block number.')
@click.option('--to-block', 'to_block', required=True, type=int, help='End block number.')
@click.option('--evm-block-delta', required=True, type=int, help='EVM block delta for action indexes.')
@click.option('--index-version', default="v1.5", show_default=True, help='Version of the index.')
def clone_and_dump_indexes(old_prefix, new_prefix, es_host, from_block, to_block, evm_block_delta, index_version):
    """
    Clones Elasticsearch indexes with a new prefix, creates a manifest file,
    dumps data and mapping using elasticdump, and creates a compressed tar.zst file.
    """
    es_client = Elasticsearch(hosts=[es_host])
    manifest = {}

    # Calculate the range of index numbers
    delta_indexes = generate_index_range(from_block, to_block, "delta", old_prefix, index_version)
    action_indexes = generate_index_range(from_block - evm_block_delta, to_block - evm_block_delta, "action", old_prefix, index_version)

    all_indexes = delta_indexes + action_indexes

    print(f"going to clone:\n{json.dumps(all_indexes, indent=4)}")

    # Check if indexes exist in Elasticsearch
    existing_indexes = check_indexes_existence(es_client, all_indexes)
    if not existing_indexes:
        print("No matching indexes found in Elasticsearch. Exiting.")
        return

    for index in existing_indexes:
        new_index = new_prefix + index[len(old_prefix):]

        set_writable(es_client, index, False)

        if not es_client.indices.exists(index=new_index):
            es_client.indices.clone(index=index, target=new_index)
            print(f"Cloned index {index} to {new_index}.")

        else:
            print(f'Skipping clone {index} to {new_index}, exists.')

        set_writable(es_client, new_index, True)

        # Delete documents out of range
        if "action" in new_index:
            delete_docs_out_of_range(es_client, new_index, "@raw.block", min_block=from_block-evm_block_delta)
            delete_docs_out_of_range(es_client, new_index, "@raw.block", max_block=to_block-evm_block_delta)

        elif "delta" in new_index:
            delete_docs_out_of_range(es_client, new_index, "block_num", min_block=from_block)
            delete_docs_out_of_range(es_client, new_index, "block_num", max_block=to_block)
    
        set_writable(es_client, new_index, False)

        doc_count = es_client.cat.count(index=new_index, format="json")
        size = int(doc_count[0]['count'])

        manifest[new_index] = {
            "mapping": new_index + "-mapping.json",
            "data": new_index + "-data.json",
            "size": size
        }

    temp_dir = os.path.join(os.getcwd(), new_prefix)

    # Create temporary directory if it doesn't exist
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    else:
        print(f"Temporary directory {temp_dir} already exists. Using existing directory.")

    try:
        manifest_path = os.path.join(temp_dir, "manifest.json")
        with open(manifest_path, 'w') as file:
            json.dump(manifest, file, indent=4)

        for new_index, files in manifest.items():
            mapping_file = os.path.join(temp_dir, files["mapping"])
            data_file = os.path.join(temp_dir, files["data"])

            # Dumping mapping and streaming output
            run_elasticdump(f"{es_host}/{new_index}", mapping_file, "mapping")

            # Dumping data and streaming output
            run_elasticdump(f"{es_host}/{new_index}", data_file, "data", limit="4000")

        # Create a compressed tar file of the temporary directory
        output_tar_zst = os.path.join(os.getcwd(), new_prefix + "_data.tar.zst")
        compress_directory(temp_dir, output_tar_zst)
        print(f"Compressed tar.zst file created at {output_tar_zst}")

    finally:
        # Optionally: Clean up the temporary directory if desired
        # Uncomment the following line if you want to delete the temp directory after use
        # shutil.rmtree(temp_dir)
        pass

def generate_index_range(start_block, end_block, index_type, prefix, version):
    docs_per_index = 10 ** 7
    return [f"{prefix}-{index_type}-{version}-{str(block_num).zfill(8)}" for block_num in range(start_block // docs_per_index, end_block // docs_per_index + 1)]

def check_indexes_existence(es_client, indexes):
    existing_indexes = []
    for index in indexes:
        if es_client.indices.exists(index=index):
            existing_indexes.append(index)
        else:
            print(f"Index {index} does not exist.")
    return existing_indexes

def set_writable(es_client, index_name, writable):
    settings = {
        "index": {
            "blocks.write": not writable
        }
    }

    # Update index settings
    response = es_client.indices.put_settings(index=index_name, body=settings)
    print(response)

def delete_docs_out_of_range(es_client, index, range_field, min_block=None, max_block=None, batch_size=100000):
    """
    Deletes documents from an index that are outside of the specified block range in batches using a scroll request.
    :param es_client: Elasticsearch client object
    :param index: Index to perform the delete operation on
    :param range_field: The field in the documents representing the block number
    :param min_block: Minimum block number (documents with block number less than this will be deleted)
    :param max_block: Maximum block number (documents with block number greater than this will be deleted)
    :param batch_size: Number of documents to delete in each batch
    :param scroll: Time to keep the scroll window open
    """
    query = {"range": {range_field: {}}}
    if min_block is not None:
        query["range"][range_field]["lt"] = min_block
    if max_block is not None:
        query["range"][range_field]["gt"] = max_block

    initial_count = es_client.count(index=index, query=query)['count']
    deleted_count = 0

    print(f"Initial count: {initial_count}")
    print(f"Query:\n{json.dumps(query, indent=4)}")

    while True:
        response = es_client.delete_by_query(
            index=index,
            query=query,
            max_docs=batch_size,
            refresh=True
        )
        batch_deleted = response['deleted']
        deleted_count += batch_deleted

        if batch_deleted == 0:
            break

        remaining = initial_count - deleted_count
        progress = (deleted_count / initial_count) * 100 if initial_count > 0 else 100
        print(f"Progress: {progress:.2f}% - Deleted {deleted_count} documents, {remaining} remaining")

    print(f"Completed trimming of index {index}. Total deleted: {deleted_count}")

def run_elasticdump(input_url, output_file, dump_type, limit=None):
    cmd = ["elasticdump", "--input", input_url, "--output", output_file, "--type", dump_type]
    if limit:
        cmd.extend(["--limit", limit])
    subprocess.run(cmd, text=True)


def compress_directory(input_path, output_path):
    with tempfile.NamedTemporaryFile(delete=False) as temp_tar:
        with tarfile.open(fileobj=temp_tar, mode='w') as tar:
            tar.add(input_path, arcname=os.path.basename(input_path))

        with open(temp_tar.name, 'rb') as src, open(output_path, 'wb') as dst:
            cctx = zstd.ZstdCompressor()
            with cctx.stream_writer(dst) as compressor:
                compressor.write(src.read())

if __name__ == '__main__':
    clone_and_dump_indexes()
