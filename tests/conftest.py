#!/usr/bin/env python3

from datetime import datetime, timedelta
import os
import logging
import threading
import subprocess

import pytest
import logging

from elasticsearch import Elasticsearch

from tevmc.config import local
from tevmc.testing import bootstrap_test_stack


def get_suffix(block_num: int, docs_per_index: int):
    return str(int(block_num / float(docs_per_index))).zfill(8)


class ProcessTimeout(Exception):
    pass


def stream_process_output(proc, message):
    for line in iter(proc.stdout.readline, ''):
        logging.info(line.rstrip())
        if message in line:
            return


@pytest.fixture(scope='module')
def elastic(request, tmp_path_factory):
    request.applymarker(pytest.mark.config(**local.default_config))
    request.applymarker(pytest.mark.services('elastic'))
    request.applymarker(pytest.mark.randomize(False))
    with bootstrap_test_stack(request, tmp_path_factory) as tevmc:
        yield tevmc

@pytest.fixture
def init_db_and_run_translator(elastic, request):
    tevmc = elastic

    docs_per_index = request.node.get_closest_marker("docs_per_index")
    if docs_per_index is None:
        docs_per_index = 10_000_000
    else:
        docs_per_index = docs_per_index.args[0]

    delta_index_spec = request.node.get_closest_marker("delta_index_spec")
    if delta_index_spec is None:
        delta_index_spec = 'delta-v1.5'
    else:
        delta_index_spec = delta_index_spec.args[0]

    action_index_spec = request.node.get_closest_marker("action_index_spec")
    if action_index_spec is None:
        action_index_spec = 'action-v1.5'
    else:
        action_index_spec = action_index_spec.args[0]

    start_time = request.node.get_closest_marker("start_time")
    if start_time is None:
        start_time = datetime.now()
    else:
        start_time = start_time.args[0]

    timeout = request.node.get_closest_marker("timeout")
    if timeout is None:
        timeout = 20
    else:
        timeout = timeout.args[0]

    txs = request.node.get_closest_marker("txs")
    if txs is None:
        txs = []
    else:
        txs = txs.args[0]

    ranges = request.node.get_closest_marker("ranges")
    if ranges is None:
        ranges = []
    else:
        ranges = ranges.args[0]

    message = request.node.get_closest_marker("message")
    if message is None:
        raise ValueError(
            'message mark required, did you forgot to mark the test?')
    else:
        message = message.args[0]

    rpc_conf = tevmc.config['telos-evm-rpc']

    es_config = tevmc.config['elasticsearch']
    es = Elasticsearch(
        f'{es_config["protocol"]}://{es_config["host"]}',
        basic_auth=(
            es_config['user'], es_config['pass']
        )
    )
    es.indices.delete(
        index=f'{rpc_conf["elastic_prefix"]}-{action_index_spec}**'
    )
    es.indices.delete(
        index=f'{rpc_conf["elastic_prefix"]}-{delta_index_spec}-*',
    )

    ops = []
    for rstart, rend in ranges:
        for i in range(rstart, rend + 1, 1):
            delta_index = f'{rpc_conf["elastic_prefix"]}-{delta_index_spec}-{get_suffix(i, docs_per_index)}'
            ops.append({
                "index": {
                    "_index": delta_index
                }
            })
            ops.append({
                "@timestamp": start_time + (i * timedelta(seconds=0.5)),
                "@global": {
                    "block_num": i
                },
                "block_num": i - 10
            })

    for tx in txs:
        action_index = f'{rpc_conf["elastic_prefix"]}-{action_index_spec}-{get_suffix(tx["@raw.block"], docs_per_index)}'
        ops.append({
            "index": {
                "_index": action_index
            }
        })
        ops.append(tx)

    es.bulk(operations=ops, refresh=True)

    env = {
        'LOG_LEVEL': 'debug',
        'CHAIN_NAME': rpc_conf["elastic_prefix"]
    }

    env.update(os.environ)

    proc = subprocess.Popen(
        ['node', 'build/main.js', '--skip-start-block-check'],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        encoding='utf-8',
        env=env
    )

    thread = threading.Thread(target=stream_process_output, args=(proc, message))
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        proc.terminate()
        thread.join()  # ensure the process has terminated before raising the exception
        raise ProcessTimeout(f"Process did not finish within {timeout} seconds")

    yield
