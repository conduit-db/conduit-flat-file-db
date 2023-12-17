# ConduitFlatFileDB

**ConduitFlatFileDB** is optimized for use as an append-only BLOB 
storage abstraction which writes directly to flat files. Deletion
is actually possible with some caveats & requiring great caution.

It is safe for access from multiple threads and multiple processes.
Lock-free read access to immutable files (which will comprise the vast 
majority of the database) is allowed if you have the context to know 
that it is safe to do so.

This is a primitive storage abstraction & would generally require coupling 
with a key value store to persist the `DataLocation` of each `put`.

## Design Overview
Writes can only occur to a single designated "mutable_file" in append only mode.
The mutable file is simply the .dat file with the highest sequence number.
Synchronization uses a single file lock for controlling access to this file.

All other data files of size >= MAX_DAT_FILE_SIZE, are marked immutable and 
concurrent readers do not require any file locking or synchronization. 
The only time readers require synchronization is when they are reading an 
entry from the "mutable_file".

The API allows for reading arbitrarily small slices into each BLOB of data.
This is true whether or not compression with zstandard is active.

There are many BLOB values per file to avoid writing many thousands of small pieces
of data to separate locations thereby causing magnetic drives to stutter.

Zstandard compression can optionally be applied to all files with the SeekableZstdFile
which maintains an index of all compression frames so that only the necessary frames
need to be decompressed to access your slice of data.

The "mutable_file" will be written to until its size is >= MAX_DAT_FILE_SIZE.
It will then open a new "mutable_file" with incremented sequence number. 
The old mutable file can now be considered to be immutable.

Deleting entries can only occur by deleting an entire file. There is no update functionality.
Deleting the mutable file is never allowed but immutable files can be deleted.
Deleting files requires great care because it will delete the data for multiple entries.

InterProcessReaderWriterLock can do around 10000 lock/unlock cycles per second on an
NVMe SSD drive. This far exceeds the maximum IOPS of a spinning HDD which is on the order of
200/second. For this reason, it is a good idea to place the file lock on an SSD as well as
the key value store for recording the DataLocation of each BLOB.


<table>
  <tr>
    <td><b>Licence</b></td>
    <td>MIT</td>
  </tr>
  <tr>
    <td><b>Language</b></td>
    <td>Python 3.10</td>
  </tr>
  <tr>
    <td><b>Author</b></td>
    <td>Hayden Donnelly (AustEcon)</td>
  </tr>
</table>

# Basic Usage

```python

import logging
from pathlib import Path

from conduit_flat_file_db.flat_file_db import FlatFileDb, FlatFileDbUnsafeAccessError
logging.basicConfig(level=logging.DEBUG)
ffdb = FlatFileDb(
    datadir=Path("./blob_store1"),
    mutable_file_lock_path=Path("./blob_store1.lock"),
    fsync=True,
    use_compression=True,
)
ffdb.MAX_DAT_FILE_SIZE = 128 * 1024 * 1024  # This is the default, uncompressed .dat file size
with ffdb:
    # Write BLOBS
    data_location_aa = ffdb.put(b"a" * (ffdb.MAX_DAT_FILE_SIZE // 16))
    data_location_bb = ffdb.put(b"b" * (ffdb.MAX_DAT_FILE_SIZE // 16))

    # Read BLOBS
    data_aa = ffdb.get(data_location_aa)
    assert data_aa == b"a" * (ffdb.MAX_DAT_FILE_SIZE // 16), data_aa
    data_bb = ffdb.get(data_location_bb)
    assert data_bb == b"b" * (ffdb.MAX_DAT_FILE_SIZE // 16), data_bb
    
    # Delete BLOBS - in general, great caution needs to be exercised for deletions
    # because multiple puts usually will share a single .dat (uncompressed) or .dat.zst (compressed) file.
    try:
        ffdb.delete_file(Path(data_location_aa.file_path))
    except FlatFileDbUnsafeAccessError as e:
        print(f"Error: {e}")  # "Error: Deleting the mutable file is not allowed. Ever."

    # Usually a new mutable file is created automatically when the last one gets full
    # But here we will force the creation of a new mutable file for demonstration
    ffdb._maybe_get_new_mutable_file(force_new_file=True)
    # data_location_aa & data_location_bb that share the same file are now in an
    # immutable file so can be deleted without `FlatFileDbUnsafeAccessError` being raised
    ffdb.delete_file(Path(data_location_aa.file_path))  # OK to delete now
    ffdb.delete_file(Path(data_location_bb.file_path))  # OK to delete now
```


# Development & Contributing
All commits should ideally pass through these checks first:

- pylint
- mypy type checker

Unit tests:

- pytest standard unittests

## Windows
There are a number of scripts provided to manage these testing procedures locally. Run them in numbered order.
Their function should be fairly self-explanatory.

-  `./scripts/0_win_build_base_image.bat`  <- only need to run this once in a while if 3rd party dependencies change
-  `./scripts/1_win_run_static_checks.bat`
-  `./scripts/2_win_run_tests.bat`
