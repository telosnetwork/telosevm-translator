#!/usr/bin/env python3

from hashlib import sha256

import pytest


@pytest.mark.ranges([
    (1, 2),
    (2, 10)
])
@pytest.mark.message('block duplicates found: [2]')
def test_dup_block_check_simple(init_db_and_run_translator):
    ...


@pytest.mark.ranges([
    (1, 200),
    (150, 300)
])
@pytest.mark.message('tx duplicates found: {str([i for range(150, 201, 1)])}')
def test_dup_block_check_multi(init_db_and_run_translator):
    ...



test_hash = sha256(b'test_tx').hexdigest()
@pytest.mark.ranges([
    (110, 120),
])
@pytest.mark.txs([
    {'@raw.block': 110, '@raw.hash': test_hash},
    {'@raw.block': 115, '@raw.hash': test_hash}
])
@pytest.mark.message(f'tx duplicates found: [\"{test_hash}\"]')
def test_dup_tx_check(init_db_and_run_translator):
    ...
