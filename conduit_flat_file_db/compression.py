import json
import logging
import os
import shutil
from pathlib import Path
from typing import TypedDict

import bitcoinx
import pyzstd
from pyzstd import CParameter, SeekableZstdFile

from .types import DataLocation

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger("zstd-compression")

# Smaller frame sizes trade compression ratio for better random read access
MAX_ZSTANDARD_FRAME_SIZE = 10 * 1024 * 1024
ZSTANDARD_LEVEL = 3


class CompressionBlockInfo(TypedDict):
    block_id: str
    tx_count: int
    size_mb: int


class CompressionStats:
    def __init__(
        self,
        filename: str = "",
        block_metadata: list[CompressionBlockInfo] | None = None,
        uncompressed_size: int = 0,
        compressed_size: int = 0,
        fraction_of_compressed_size: float = 0.0,
    ):
        assert isinstance(filename, str)
        self.filename = filename
        self.block_metadata = block_metadata if block_metadata is not None else []
        self.uncompressed_size = uncompressed_size
        self.compressed_size = compressed_size
        self.fraction_of_compressed_size = fraction_of_compressed_size
        self.max_window_size = MAX_ZSTANDARD_FRAME_SIZE
        self.zstandard_level = ZSTANDARD_LEVEL

    def __repr__(self) -> str:
        return (
            f'CompressionStats(filename={self.filename!r}, '
            f'block_metadata={self.block_metadata!r}, '
            f'uncompressed_size={self.uncompressed_size}, '
            f'compressed_size={self.compressed_size}, '
            f'fraction_of_compressed_size={self.fraction_of_compressed_size}, '
            f'max_window_size={self.max_window_size}, '
            f'zstandard_level={self.zstandard_level})'
        )

    def __str__(self) -> str:
        return (
            f'CompressionStats: filename={self.filename}, '
            f'uncompressed_size={self.uncompressed_size}, '
            f'compressed_size={self.compressed_size}, '
            f'fraction_of_compressed_size={self.fraction_of_compressed_size}, '
            f'max_window_size={self.max_window_size}, '
            f'zstandard_level={self.zstandard_level}'
        )

    def to_json(self) -> str:
        data = {
            "filename": str(self.filename),
            "block_metadata": [block for block in self.block_metadata],
            "uncompressed_size": self.uncompressed_size,
            "compressed_size": self.compressed_size,
            "fraction_of_compressed_size": self.fraction_of_compressed_size,
            "max_window_size": self.max_window_size,
            "zstandard_level": self.zstandard_level,
        }
        return json.dumps(data)


def open_seekable_writer_zstd(filepath: Path, mode: str = 'ab') -> SeekableZstdFile:
    option = {CParameter.compressionLevel: ZSTANDARD_LEVEL, CParameter.checksumFlag: 1}
    return SeekableZstdFile(
        filepath, mode=mode, level_or_option=option, max_frame_content_size=MAX_ZSTANDARD_FRAME_SIZE
    )


def open_seekable_reader_zstd(filepath: Path) -> SeekableZstdFile:
    return SeekableZstdFile(filepath, mode='rb')


def write_to_file_zstd(
    filepath: Path,
    batch: list[bytes],
    fsync: bool = False,
    compression_stats: CompressionStats | None = None,
    mode: str = 'ab',
) -> list[DataLocation]:
    """This is batched for greatly improved efficiency with multiple, small writes.

    compression_stats can optionally be provided and will be updated in-place
    (rather than returned). This is to not break the preferred return type / API of this function"""
    if mode == 'ab':
        # Cannot get this information from an append mode SeekableZstdFile
        with open_seekable_reader_zstd(filepath) as seekable_reader:
            uncompressed_start_offset = seekable_reader.seek(0, os.SEEK_END)
            seekable_reader.seek(0)
    elif mode == 'wb':
        uncompressed_start_offset = 0
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    with open_seekable_writer_zstd(filepath, mode) as seekable_zstd:
        compressed_start_offset = seekable_zstd._fp.tell()  # type: ignore[attr-defined]

        data_locations = []
        file_start_offset_within_file = uncompressed_start_offset
        for data in batch:
            seekable_zstd.write(data)
            file_end_offset_within_file = file_start_offset_within_file + len(data)
            data_locations.append(
                DataLocation(str(filepath), file_start_offset_within_file, file_end_offset_within_file)
            )
            file_start_offset_within_file = file_end_offset_within_file
        seekable_zstd.flush(SeekableZstdFile.FLUSH_FRAME)
        if fsync:
            os.fsync(seekable_zstd.fileno())

        if compression_stats:
            uncompressed_size = file_end_offset_within_file - uncompressed_start_offset
            compressed_size = seekable_zstd._fp.tell() - compressed_start_offset  # type: ignore[attr-defined]
            update_compresson_stats(filepath, uncompressed_size, compressed_size, compression_stats)

    return data_locations


def uncompressed_file_size_zstd(file_path: str) -> int:
    with SeekableZstdFile(file_path, 'rb') as file:
        return file.seek(0, os.SEEK_END)


def update_compresson_stats(
    filepath: Path, uncompressed_size: int, compressed_size: int, compression_stats: CompressionStats
) -> CompressionStats:
    compression_stats.filename = str(filepath.name)
    compression_stats.uncompressed_size = compression_stats.uncompressed_size + uncompressed_size
    compression_stats.compressed_size = compression_stats.compressed_size + compressed_size
    compression_stats.fraction_of_compressed_size = (
        compression_stats.compressed_size / compression_stats.uncompressed_size
    )
    return compression_stats


def write_compression_stats(compression_stats: CompressionStats) -> None:
    datadir = Path(os.getenv('DATADIR_SSD', str(MODULE_DIR)))
    compression_stats_file_path = datadir / "compression_stats.json"

    mode = 'a' if compression_stats_file_path.exists() else 'w'
    with open(compression_stats_file_path, mode) as file:
        file.write(compression_stats.to_json() + "\n")


def check_and_recover_zstd_file(
    input_file_path: str, output_file_path: str = 'recovery_file.dat', worker_id: str = ""
) -> None:
    logger_name = "zstd-compression" if not worker_id else f"zstd-compression-{worker_id}"
    logger = logging.getLogger(logger_name)

    logger.debug(f"Running integrity checks on {input_file_path}.")
    try:
        with open(input_file_path, 'rb') as ifh:
            with open(output_file_path, 'wb') as ofh:
                pyzstd.decompress_stream(ifh, ofh)
    except pyzstd.ZstdError as e:
        logger.error(str(e))

    repairs_needed = reparse_and_remove_incomplete_blocks(Path(output_file_path))
    if not repairs_needed:
        logger.info(f"ZStandard archive passed all integrity checks")
        Path(output_file_path).unlink()
        return
    logger.info(f"Re-checking the file...")
    repairs_needed = reparse_and_remove_incomplete_blocks(Path(output_file_path))
    assert repairs_needed is False

    # Keep a .bak copy just in case...
    shutil.move(src=input_file_path, dst=input_file_path + ".bak")
    logger.info(f"Re-creating {input_file_path} without the corrupted last frame")
    with open(output_file_path, 'rb') as recovery_file:
        with pyzstd.SeekableZstdFile(input_file_path, mode='wb') as file_zstd:
            while True:
                data = recovery_file.read(1024 * 1024 * 128)
                if not data:
                    break
                file_zstd.write(data)
    Path(output_file_path).unlink()
    # Need to remove this because it interferes with the
    # mutable file numbering system
    Path(input_file_path + ".bak").unlink()


def reparse_and_remove_incomplete_blocks(file_path: Path) -> bool:
    """This will re-parse all transactions in all bitcoin blocks to ensure they are
    complete and valid. If it encounters partial block data, it gets truncated from the file"""
    stream = open(file_path, 'rb')
    file_size = file_path.stat().st_size
    if file_size == 0:
        logger.info(f"File passed all integrity checks")
        return False
    okay_up_to_offset = file_size
    total_tx_count = 0
    try:
        while True:
            header = bitcoinx.Bitcoin.deserialized_header(stream.read(80), 0)
            # Parse header and all transactions in the raw block
            tx_count = bitcoinx.read_varint(stream.read)
            for i in range(tx_count):
                tx = bitcoinx.Tx.read(stream.read)
                total_tx_count += 1

            # Checkpoint for the offset
            okay_up_to_offset = stream.tell()
            # logger.debug(f"Offset to: {okay_up_to_offset} okay.")
            if okay_up_to_offset == file_size:
                logger.info(f"File passed all integrity checks")
                repairs_needed = False
                return repairs_needed
    except Exception:
        logger.error(
            f"The remainder of this file is corrupted most likely because the last zstandard "
            f"frame for this file was not flushed. Truncating file at offset: {okay_up_to_offset}"
        )
        stream.close()
        with open(file_path, 'ab') as file:
            file.seek(okay_up_to_offset, os.SEEK_SET)
            file.truncate()
        repairs_needed = True
        return repairs_needed
    finally:
        stream.close()
