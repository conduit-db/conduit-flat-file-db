from typing import NamedTuple


class DataLocation(NamedTuple):
    """This metadata must be persisted elsewhere.
    For example, a key-value store such as LMDB"""
    file_path: str
    start_offset: int
    end_offset: int


class Slice(NamedTuple):
    start_offset: int
    end_offset: int
