# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import functools
import logging
import multiprocessing
import os
import shutil
import stat
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import time
from typing import Iterator, Callable, Any

import pytest
from _pytest.fixtures import FixtureRequest

from conduit_flat_file_db.flat_file_db import FlatFileDb, FlatFileDbUnsafeAccessError
from conduit_flat_file_db.types import Slice

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("test-flat-file-db")
TEST_DATADIR = str(MODULE_DIR / "test_path")
FFDB_LOCKFILE = os.environ["FFDB_LOCKFILE"] = "ffdb.lock"

os.makedirs(TEST_DATADIR, exist_ok=True)


def remove_readonly(func: Callable[..., Any], path: str, excinfo: Any) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)


def _do_general_read_and_write_ops(ffdb: FlatFileDb) -> FlatFileDb:
    logging.basicConfig(level=logging.DEBUG)
    # NOTE The use case of a chain indexer does not require fsync because we can always
    # re-sync from the node if we crash...
    with ffdb:
        data_location_aa = ffdb.put(b"a" * (ffdb.MAX_DAT_FILE_SIZE // 16))
        # logger.debug(data_location_aa)
        data_location_bb = ffdb.put(b"b" * (ffdb.MAX_DAT_FILE_SIZE // 16))
        # logger.debug(data_location_bb)

        # Read
        data_aa = ffdb.get(data_location_aa)
        assert data_aa == b"a" * (ffdb.MAX_DAT_FILE_SIZE // 16), data_aa
        # logger.debug(data_aa)

        data_bb = ffdb.get(data_location_bb)
        assert data_bb == b"b" * (ffdb.MAX_DAT_FILE_SIZE // 16), data_bb
        # logger.debug(data_bb)

        with ffdb.mutable_file_rwlock.write_lock():
            ffdb._maybe_get_new_mutable_file()

        return ffdb


def _task_multithreaded(ffdb: FlatFileDb, x: int) -> None:
    """Need to instantiate and pass into threads"""
    _do_general_read_and_write_ops(ffdb)


def _task(use_compression: bool, x: int) -> None:
    """Create each FlatFileDb instance anew within each process"""
    ffdb = FlatFileDb(
        datadir=Path(TEST_DATADIR),
        mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
        fsync=True,
        use_compression=use_compression,
    )
    ffdb.MAX_DAT_FILE_SIZE = 1024 * 128
    _do_general_read_and_write_ops(ffdb)


class TestFlatFileDb:
    @pytest.fixture(scope="class", params=[True, False])
    def use_compression(self, request: FixtureRequest) -> Iterator[bool]:
        if os.path.exists(TEST_DATADIR):
            shutil.rmtree(TEST_DATADIR, onerror=remove_readonly)
        yield request.param
        if os.path.exists(TEST_DATADIR):
            shutil.rmtree(TEST_DATADIR, onerror=remove_readonly)

        if os.path.exists(FFDB_LOCKFILE):
            os.remove(FFDB_LOCKFILE)

    def test_general_read_and_write_db(self, use_compression: bool) -> None:
        ffdb = FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
            fsync=True,
            use_compression=use_compression,
        )
        ffdb.MAX_DAT_FILE_SIZE = 1024
        _do_general_read_and_write_ops(ffdb)

    def test_delete(self, use_compression: bool) -> None:
        with FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
            fsync=True,
            use_compression=use_compression,
        ) as ffdb:
            data_location_aa = ffdb.put(b"a" * 10)  # immutable file
            ffdb._maybe_get_new_mutable_file(force_new_file=True)
            data_location_bb = ffdb.put(b"b" * 10)  # mutable file

            # Cannot delete the mutable file - not safe
            with pytest.raises(FlatFileDbUnsafeAccessError):
                ffdb.delete_file(Path(data_location_bb.file_path))

            # Deleting immutable files is okay (but checks should be done first to make
            # sure that ConduitIndex has parsed and check pointed for all of the raw blocks
            data = ffdb.get(data_location_aa)
            assert data == ('a' * 10).encode('utf-8')
            ffdb.delete_file(Path(data_location_aa.file_path))
            with pytest.raises(FileNotFoundError):
                ffdb.get(data_location_aa)

    def test_slicing(self, use_compression: bool) -> None:
        with FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
            fsync=True,
            use_compression=use_compression,
        ) as ffdb:
            data_location_mixed = ffdb.put(b"aaaaabbbbbccccc")
            # print(data_location_mixed)

            slice = Slice(start_offset=5, end_offset=10)
            data_bb_slice = ffdb.get(data_location_mixed, slice)
            assert data_bb_slice == b"bbbbb"
            # print(data_bb_slice)

    def test_multithreading_access(self, use_compression: bool) -> None:
        starttime = time.time()
        ffdb = FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
            fsync=True,
            use_compression=use_compression,
        )
        ffdb.MAX_DAT_FILE_SIZE = 1024 * 128
        func = functools.partial(_task_multithreaded, ffdb)
        with ThreadPoolExecutor(4) as pool:
            for result in pool.map(func, range(0, 32)):
                if result:
                    logger.debug(result)
        endtime = time.time()
        logger.debug(f"Time taken {endtime - starttime} seconds")

    def test_multiprocessing_access(self, use_compression: bool) -> None:
        starttime = time.time()
        with multiprocessing.Pool(4) as pool:
            func = functools.partial(_task, use_compression)
            pool.map(func, range(0, 32))

        endtime = time.time()
        logger.debug(f"Time taken {endtime - starttime} seconds")
