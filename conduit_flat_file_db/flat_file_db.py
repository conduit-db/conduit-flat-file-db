# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

"""
NOTE: FlatFileDb should remain decoupled from LMDB. Updates will not be allowed.
 Deletion of whole files will be the only way to remove old records (to create a rolling window
 storage) which is a good fit for the blockchain timestamp / time-series model.
"""

import logging
import os
import shutil
import threading
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Type

from fasteners import InterProcessReaderWriterLock
from pyzstd import SeekableZstdFile, seekable_zstdfile

from .compression import (
    uncompressed_file_size_zstd,
    write_to_file_zstd,
    CompressionStats,
    open_seekable_writer_zstd,
)
from .types import Slice, DataLocation

logger = logging.getLogger("lmdb-utils")

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


class FlatFileDbUnsafeAccessError(Exception):
    pass


class FlatFileDbWriteFailedError(Exception):
    pass


def write_to_file(filepath: Path, batch: list[bytes], fsync: bool = False) -> list[DataLocation]:
    with open(filepath, "ab") as file:
        data_locations = []
        for data in batch:
            start_offset = file.tell()
            file.write(data)
            end_offset = file.tell()
            data_locations.append(DataLocation(str(filepath), start_offset, end_offset))

        if fsync:
            file.flush()
            os.fsync(file.fileno())
    return data_locations


class FlatFileDb:
    """This is optimized for use as a write once, read many BLOB storage abstraction which writes
    directly to flat files. Writes can only occur to a single designated "mutable_file" in
    append only mode. Synchronization uses a single file lock for controlling access to this file.

    All other data files of size >= MAX_DAT_FILE_SIZE, are immutable and concurrent readers do not
    require any file locking or synchronization. The only time readers require synchronization is
    when they are reading an entry from the "mutable_file".

    The API allows for reading arbitrarily small slices into each BLOB of data.

    There are many BLOB values per file to avoid writing many thousands of small 1MB files and
    thereby minimising IOPS on spinning HDDs.

    Zstandard compression can optionally be applied to all files with the SeekableZstdFile

    The "mutable_file" will be written to until its size is >= MAX_DAT_FILE_SIZE.
    It will then open a new "mutable_file". The old mutable file can now be
    considered to be immutable.

    Deleting entries can only occur by deleting an entire file. There is no update functionality.

    InterProcessReaderWriterLock can do around 10000 lock/unlock cycles per second on an
    NVMe SSD drive. This far exceeds the maximum IOPS of a spinning HDD which is on the order of
    200/second. For this reason, it is a good idea to place the file lock on an SSD as well as
    the key value store for recording the DataLocation of each BLOB.

    NOTE: InterProcessReaderWriterLock is not re-entrant.
    NOTE: FlatFileDB is thread-safe but __init__ method is not. Must instantiate in parent thread
    """

    def __init__(
        self, datadir: Path, mutable_file_lock_path: Path, fsync: bool = False, use_compression: bool = False
    ) -> None:
        self.threading_lock = threading.RLock()
        self.fsync: bool = fsync
        self.datadir = datadir
        self.mutable_file_lock_path = mutable_file_lock_path
        self.use_compression = use_compression
        self.read_handles: dict[str, BinaryIO | SeekableZstdFile] = {}

        self.MAX_DAT_FILE_SIZE = 128 * (1024**2)  # 128MB
        self.MAX_OPEN_READ_HANDLES = 100

        assert str(self.mutable_file_lock_path).endswith(
            ".lock"
        ), "mutable_file_lock_path must end with '.lock'"

        if not self.datadir.exists():
            os.makedirs(self.datadir, exist_ok=True)

        # Inter-process synchronization of access to the mutable file
        # WARNING mutable_file_lock_path needs to exactly match across threads and processes
        self.mutable_file_rwlock = InterProcessReaderWriterLock(str(self.mutable_file_lock_path))

        # Initialization
        with self.mutable_file_rwlock.write_lock():
            # Create file data_00000000.dat if it's the first time opening the datadir
            self.mutable_file_path = self._file_num_to_mutable_file_path(file_num=0)
            if not self.mutable_file_path.exists():
                logger.debug(f"Initializing datadir {self.datadir} by creating" f" {self.mutable_file_path}")
                with open(self.mutable_file_path, "ab") as f:
                    f.flush()
                    os.fsync(f.fileno())

        with self.mutable_file_rwlock.read_lock():
            # Scans datadir to get the correct mutable_file_file cached properties
            _immutable_files: list[str] = os.listdir(self.datadir)
            _immutable_files.sort()
            # Pop the single mutable file (which always has the highest number)
            for file in _immutable_files:
                if file.endswith(".lock"):
                    _immutable_files.remove(file)
            mutable_filename = _immutable_files.pop()
            self.mutable_file_num = self._mutable_filename_to_num(mutable_filename)
            self.mutable_file_path = self._file_num_to_mutable_file_path(self.mutable_file_num)
            if self.use_compression:
                assert str(self.mutable_file_path).endswith(
                    ".zst"
                ), "Once compressed mode is on, you cannot switch"
                # TODO(generic-lib): Need to do a database integrity check that truncates
                #  whatever is in the last bad frame which would potentially leave some
                #  partially written data in the end of the second-to-last
                #  frame that nothing would have a reference to anymore. It would just sit there
                #  not harming anything so long as the API of FlatFileDB is used correctly
                #  and they don't reach in behind the interface and directly read the whole
                #  file themselves manually.
                #  Until there is a generic implementation, I have a Bitcoin-specific database
                #  integrity check that re-parses the raw blocks to be 100% sure.
            else:
                assert str(self.mutable_file_path).endswith(
                    ".dat"
                ), "Operating in compressed mode requires a full reindex"
            self.immutable_files = set(_immutable_files)

    def __enter__(self) -> "FlatFileDb":
        self.threading_lock.acquire()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.threading_lock.release()

    def _cleanup_partial_writes_on_last_close(self, file_path: Path) -> None:
        """This can happen if there is a partially written .zst archive that did not have the
        last frame closed and flushed"""
        try:
            file_zstd = open_seekable_writer_zstd(file_path)
            file_zstd.close()
        except seekable_zstdfile.SeekableFormatError:
            file_path.unlink()

    def _file_num_to_mutable_file_path(self, file_num: int) -> Path:
        padded_str_num = str(file_num).zfill(8)
        filename = f"data_{padded_str_num}.dat"
        if self.use_compression:
            filename += ".zst"
        assert self.datadir is not None
        return self.datadir / filename

    def _mutable_filename_to_num(self, filename: str) -> int:
        filename = filename.removeprefix("data_")
        if self.use_compression:
            filename = filename.removesuffix(".zst")
        filename = filename.removesuffix(".dat")
        return int(filename)

    def _maybe_get_new_mutable_file(self, force_new_file: bool = False) -> tuple[Path, int]:
        """This function is idempotent. Caller must use a Write lock"""
        assert self.mutable_file_path is not None

        def _mutable_file_is_full() -> bool:
            assert self.mutable_file_path is not None
            if self.use_compression:
                file_size = uncompressed_file_size_zstd(str(self.mutable_file_path))
            else:
                file_size = os.path.getsize(self.mutable_file_path)
            is_full = file_size >= self.MAX_DAT_FILE_SIZE
            return is_full

        if _mutable_file_is_full() or force_new_file:
            # If deletes and updates are allowed this needs to be more sophisticated
            # To ensure that the mutable file always has the highest number (no reuse of
            # lower mutable_file_num even if they get deleted)
            while True:
                logger.debug(f"Scanning forward... self.mutable_file_num={self.mutable_file_num}")
                self.immutable_files.add(str(self.mutable_file_path.name))
                self.mutable_file_num += 1
                self.mutable_file_path = self._file_num_to_mutable_file_path(self.mutable_file_num)

                if os.path.exists(self.mutable_file_path):
                    if _mutable_file_is_full():
                        continue
                    else:
                        self.mutable_file_path = self.mutable_file_path
                        break
                else:
                    logger.debug(f"Creating a new mutable file at: {self.mutable_file_path}")
                    if self.use_compression:
                        open_seekable_writer_zstd(self.mutable_file_path, mode='wb').close()
                    else:
                        open(self.mutable_file_path, "wb").close()
                    self.mutable_file_path = self.mutable_file_path
                    break

        return self.mutable_file_path, self.mutable_file_num

    def maybe_evict_cached_read_handles(self) -> None:
        # Evict the 10 read handles for the lowest file number
        if len(self.read_handles) > self.MAX_OPEN_READ_HANDLES:
            read_handles_list = list(self.read_handles)
            read_handles_list.sort(reverse=True)
            for i in range(10):
                key = read_handles_list.pop()
                read_handle = self.read_handles.pop(key)
                read_handle.close()

    def put_many(
        self, batch: list[bytes], compression_stats: CompressionStats | None = None
    ) -> list[DataLocation]:
        """Seekable Format files are desperately needed for improved random read access.
        See: https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md
        """
        assert self.mutable_file_path is not None
        with self.mutable_file_rwlock.write_lock():
            self._maybe_get_new_mutable_file()
            if not self.use_compression:
                data_locations = write_to_file(self.mutable_file_path, batch, self.fsync)
            else:
                data_locations = write_to_file_zstd(
                    self.mutable_file_path, batch, self.fsync, compression_stats
                )
            return data_locations

    def put(self, data: bytes) -> DataLocation:
        """Use of this method is discouraged"""
        return self.put_many([data])[0]

    def put_big_block(self, data_location: DataLocation) -> DataLocation:
        """This should return almost instantly because it's only renaming a file on the same disc"""
        assert self.mutable_file_path is not None
        with self.mutable_file_rwlock.write_lock():
            self._maybe_get_new_mutable_file(force_new_file=True)
            shutil.move(src=data_location.file_path, dst=self.mutable_file_path)
            return DataLocation(
                str(self.mutable_file_path),
                data_location.start_offset,
                data_location.end_offset,
            )

    def get_read_handle(self, read_path: str | Path) -> BinaryIO | SeekableZstdFile:
        read_path = str(read_path)
        if self.use_compression:
            read_handle = self.read_handles.get(read_path)
            if not read_handle:
                self.maybe_evict_cached_read_handles()
                read_handle = SeekableZstdFile(read_path, mode='rb')
                self.read_handles[read_path] = read_handle
        else:
            read_handle = self.read_handles.get(read_path)
            if not read_handle:
                self.maybe_evict_cached_read_handles()
                read_handle = open(read_path, 'rb')
                self.read_handles[read_path] = read_handle
        return read_handle

    def get(
        self,
        data_location: DataLocation,
        slice: Slice | None = None,
        lock_free_access: bool = False,
    ) -> bytes:
        """If the end offset of the Slice is zero, it reads to the end of the
        data location

        lock_free_access=True is only safe if there are no concurrent deletes.
        If this is used as a write once, read many database, this assumption holds and the
        advantage is unfettered lock-free read access to all immutable files. A lock must ALWAYS
        be acquired for reading the mutable file - this can never be disabled.
        """
        is_accessing_mutable_file = Path(data_location.file_path).name not in self.immutable_files

        if is_accessing_mutable_file or not lock_free_access:
            self.mutable_file_rwlock.acquire_read_lock()

        read_path, start_offset, end_offset = data_location
        read_handle = self.get_read_handle(read_path)
        try:
            read_handle.seek(start_offset)
            full_data_length = end_offset - start_offset
            if not slice:
                return read_handle.read(full_data_length)

            start_offset_within_entry = start_offset + slice.start_offset
            if slice.end_offset == 0:
                end_offset_within_entry = end_offset
            else:
                end_offset_within_entry = start_offset + slice.end_offset
            read_handle.seek(start_offset_within_entry)
            len_data = end_offset_within_entry - start_offset_within_entry
            return read_handle.read(len_data)
        except OSError:
            logger.error(f"Error reading from file_path: {data_location.file_path}")
            raise FileNotFoundError(f"Error reading from file_path: {data_location.file_path}")
        finally:
            if is_accessing_mutable_file:
                read_handle.close()
                del self.read_handles[read_path]

            if is_accessing_mutable_file or not lock_free_access:
                self.mutable_file_rwlock.release_read_lock()

    def delete_file(self, file_path: Path) -> None:
        """Exercise extreme caution with this method. A single file can contain multiple records
        at different slice offsets. This will delete the whole data file.

        It is important that no lock_free_access=True type reads are being
        attempted when deleting a file. lock_free_access=True requires taking on this
        responsibility in return for better performance.
        """
        with self.mutable_file_rwlock.write_lock():
            try:
                logger.debug(f"Deleting file: {file_path}")
                logger.debug(f"Current mutable file: {self.mutable_file_path}")
                if self.mutable_file_path == file_path:
                    raise FlatFileDbUnsafeAccessError("Deleting the mutable file is not allowed. Ever.")
                    # self._maybe_get_new_mutable_file(force_new_file=True)
                assert (
                    file_path.name in self.immutable_files
                ), f"{file_path.name} not in {self.immutable_files}"
                read_handle = self.read_handles.get(str(file_path))
                if read_handle:
                    read_handle.close()
                    del self.read_handles[str(file_path)]
                if self.use_compression:
                    assert file_path.name.endswith(".zst")
                else:
                    assert file_path.name.endswith(".dat")
                os.remove(file_path)
                logger.info(f"File deleted at path: {file_path}")
            except FileNotFoundError:
                logger.error(f"File not found at path: {file_path}. Was it already deleted?")

    def uncompressed_file_size(self, file_path: str) -> int:
        if self.use_compression:
            uncompressed_size = uncompressed_file_size_zstd(file_path)
        else:
            uncompressed_size = os.path.getsize(file_path)
        return uncompressed_size
