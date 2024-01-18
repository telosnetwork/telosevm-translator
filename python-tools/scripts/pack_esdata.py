import os
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

    # Check if indexes exist in Elasticsearch
    existing_indexes = check_indexes_existence(es_client, all_indexes)
    if not existing_indexes:
        print("No matching indexes found in Elasticsearch. Exiting.")
        return

    for index in existing_indexes:
        new_index = new_prefix + index[len(old_prefix):]
        es_client.indices.clone(index=index, target=new_index)
        print(f"Cloned index {index} to {new_index}")

        doc_count = es_client.cat.count(new_index, params={"format": "json"})
        size = int(doc_count[0]['count'])

        manifest[new_index] = {
            "mapping": new_index + "-mapping.json",
            "data": new_index + "-data.json",
            "size": size
        }

    with tempfile.TemporaryDirectory() as temp_dir:
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

        output_tar_zst = os.path.join(os.getcwd(), new_prefix + "_data.tar.zst")
        compress_directory(temp_dir, output_tar_zst)
        print(f"Compressed tar.zst file created at {output_tar_zst}")

def generate_index_range(start_block, end_block, index_type, prefix, version):
    return [f"{prefix}-{index_type}-{version}-{str(block_num).zfill(8)}" for block_num in range(start_block // 10**8, end_block // 10**8 + 1)]

def check_indexes_existence(es_client, indexes):
    existing_indexes = []
    for index in indexes:
        if es_client.indices.exists(index=index):
            existing_indexes.append(index)
        else:
            print(f"Index {index} does not exist.")
    return existing_indexes

def run_elasticdump(input_url, output_file, dump_type, limit=None):
    cmd = ["elasticdump", "--input", input_url, "--output", output_file, "--type", dump_type]
    if limit:
        cmd.extend(["--limit", limit])
    subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

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
