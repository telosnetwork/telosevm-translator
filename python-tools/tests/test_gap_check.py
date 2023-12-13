#!/usr/bin/env python3

import pytest


@pytest.mark.ranges([
    (1, 2),
    (4, 10)
])
@pytest.mark.message('Gap in database found at 2')
def test_gap_check_bsp_early(init_db_and_run_translator):
    ...

@pytest.mark.ranges([
    (1, 4),
    (6, 10)
])
@pytest.mark.message('Gap in database found at 4')
def test_gap_check_bsp_middle(init_db_and_run_translator):
    ...

@pytest.mark.ranges([
    (1, 6),
    (8, 10)
])
@pytest.mark.message('Gap in database found at 6')
def test_gap_check_bsp_late(init_db_and_run_translator):
    ...

@pytest.mark.ranges([
    (9_999_900, 9_999_995),
    (10_000_005, 10_000_100)
])
@pytest.mark.message('Gap in database found at 9999995')
def test_gap_check_between_two_indices(init_db_and_run_translator):
    ...

@pytest.mark.ranges([
    (9_999_900, 9_999_999),
    (20_000_000, 20_000_100)
])
@pytest.mark.message('Gap in database found at 9999999')
def test_gap_check_large_gap(init_db_and_run_translator):
    ...

@pytest.mark.ranges([
    (9_999_900, 9_999_999),
    (100_000_000, 100_000_100)
])
@pytest.mark.message('Gap in database found at 9999999')
def test_gap_check_very_large_gap(init_db_and_run_translator):
    ...
