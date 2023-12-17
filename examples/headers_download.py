import asyncio
import sys
from io import BytesIO
import logging

import bitcoinx
from bitcoinx import Headers, MissingHeader, hex_str_to_hash

from conduit_flat_file_db import BitcoinClient, BitcoinClientManager, HandlersDefault
from conduit_flat_file_db.constants import ZERO_HASH, TESTNET
from conduit_flat_file_db.types import InvType

logging.basicConfig(format="%(asctime)-25s %(levelname)-10s %(name)-28s %(message)s")


async def get_max_headers(client: BitcoinClient, headers_store: Headers) -> bytes | None:
    block_locator_hashes = []
    tip_height = headers_store.longest_chain().tip.height
    for i in range(0, 25, 2):
        height = tip_height - i**2
        if i == 0 or height > 0:
            locator_hash = headers_store.header_at_height(headers_store.longest_chain(), height).hash
            block_locator_hashes.append(locator_hash)
    hash_count = len(block_locator_hashes)
    return await client.get_headers(hash_count, block_locator_hashes, ZERO_HASH)


def connect_headers(stream: BytesIO, genesis_block_hash: str, headers_store: Headers,
        logger: logging.Logger):
    count = bitcoinx.read_varint(stream.read)
    for i in range(count):
        try:
            raw_header = stream.read(80)
            _tx_count = bitcoinx.read_varint(stream.read)
            headers_store.connect(raw_header)
        except MissingHeader as e:
            if str(e).find(genesis_block_hash) != -1:
                logger.debug("skipping prev_out == genesis block")
                continue
            else:
                logger.error(e)
    headers_store.flush()


def have_header(block_hash: bytes, headers_store: Headers) -> bool:
    try:
        headers_store.lookup(hex_str_to_hash(block_hash))
        return True
    except bitcoinx.MissingHeader:
        return False


async def main():
    logger = logging.getLogger("main")
    logger.setLevel(logging.DEBUG)

    # Create bitcoinx.Headers store
    import bitcoinx
    from bitcoinx import CheckPoint

    if bitcoinx._version_str != "0.7.1":
        print("This script requires bitcoinx version: 0.7.1")
        sys.exit(1)

    # i.e. Genesis block
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "010000000000000000000000000000000000000000000000000000000000000000000000"
            "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494d"
            "ffff001d1aa4ae18"
        ),
        height=0,
        prev_work=0,
    )
    headers_store = bitcoinx.Headers(bitcoinx.BitcoinTestnet, "headers.mmap", CHECKPOINT)

    # Connect to testnet peers and download all headers to chain tip
    message_handler = HandlersDefault(TESTNET)
    peers_list = ["136.243.78.45:18333", "142.132.159.187:18333"]  # good peers
    peers_list_bad_peers = ["127.0.0.1:12345"]
    peers_list.extend(peers_list_bad_peers)

    async with BitcoinClientManager(message_handler, peers_list,
            use_headers_queue=True, use_inv_queue=True) as client_manager:
        client = await client_manager.get_next_available_peer()

        # Initial headers download
        while headers_store.longest_chain().tip.height < client.remote_start_height:
            # round-robin switching between peers & skip bad or disconnected peers
            # this doesn't actually make any sense for fetching headers but it showcases
            # that you could for example load balance fetching of raw blocks or iterate
            # through all available peers to broadcast a transaction.
            client = await client_manager.get_next_available_peer()
            headers_message = await get_max_headers(client, headers_store)  # Gets ignored when node is in IBD
            if headers_message:
                connect_headers(BytesIO(headers_message),
                    client_manager.net_config.GENESIS_BLOCK_HASH, headers_store, logger)
                logger.info(
                    f"New headers tip height: %s (peer={client.id})",
                    headers_store.longest_chain().tip.height,
                )
            else:
                logger.debug("No headers returned (peer_id=%s)", client.id)

        logger.info(
            f"Finished initial headers download. New headers tip height: %s (peer={client.id})",
            headers_store.longest_chain().tip.height,
        )
        logger.info(f"Waiting for new chain tip")
        while True:
            message = await client.inv_queue.get()
            inv_vect = client.deserializer.inv(BytesIO(message))
            for inv in inv_vect:
                if inv["inv_type"] == InvType.BLOCK:
                    if not have_header(inv['inv_hash'], headers_store):
                        headers_message = await get_max_headers(client,
                            headers_store)
                        break
            else:
                continue

            if headers_message:
                connect_headers(BytesIO(headers_message),
                    client_manager.net_config.GENESIS_BLOCK_HASH, headers_store, logger)
                logger.info(
                    f"New headers tip height: %s (peer={client.id})",
                    headers_store.longest_chain().tip.height,
                )

asyncio.run(main())
