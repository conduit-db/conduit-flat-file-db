import asyncio
from io import BytesIO
import logging

from bitcoinx import hash_to_hex_str

from conduit_flat_file_db import BitcoinClientManager, REGTEST, HandlersDefault
from conduit_flat_file_db.constants import ZERO_HASH
from conduit_flat_file_db.types import BlockHeader, BitcoinClientMode, BroadcastResult
from tests_functional.conftest import call_any

logging.basicConfig(format="%(asctime)-25s %(levelname)-10s %(name)-28s %(message)s")


async def main():
    # There a quirk with the p2p network where a node will only respond with a rejection
    # on the first time for the current block. On subsequent broadcasts there will be
    # radio silence which can be misinterpreted as implicit acceptance
    # I want this example to behave as expected, so I mine a new regtest block each time.
    call_any('generate', 1)

    logger = logging.getLogger('main')
    logger.setLevel(logging.DEBUG)
    message_handler = HandlersDefault(REGTEST)
    peers_list = ["127.0.0.1:18444", "127.0.0.1:18444", "127.0.0.1:18444"]

    async with BitcoinClientManager(message_handler, peers_list,
            mode=BitcoinClientMode.SIMPLE) as client_manager:

        # ccode_translation=REJECT_MALFORMED
        rawtx: bytes = b"raw transaction goes here"
        result: BroadcastResult = await client_manager.broadcast_transaction_and_listen(rawtx,
            broadcast_peer_count=1)
        if result.rejected is not None:
            logger.error(f"Transaction rejected. Reason: {result.rejected}")
        else:
            logger.error(f"Transaction accepted. Relaying peer_ids: {result.relaying_peer_ids}")

        # Random tx from testnet (not regtest)
        # ccode_translation=REJECT_NONSTANDARD
        rawtx: bytes = bytes.fromhex("020000000217de77d1421d6a6fff4a752524a9b810de8f407a8d833ed7b58f9843efe25853000000006b483045022100ed004a135f1ba01cf163beaff04662fc821581ac8ffeb3e1852e5ebdea82ab5d022052f0c8281c7f934d36667ba0770762228708d6b7e0bcd5463a777276080b9728412103f81d6e9176b7dfe9e86728d3b721ffd5ab9bdb9327be80b6824b8fd35cad96e3feffffff62f233fd8668e4722244279ccc1c94dc809f3cedca7e384795f7ba7e1f69e3df010000006b4830450221008e65c2867f7f4e5675242ba00eaaf98d21b338932c5574568f37b68ec438a82302205c1269ba81e53ea9f3755935e40813c62c9eaeb312888dd9b4b9f1a97d443198412103623a72e6962ccdd1e0793f87ca0af381e190a8857753b4c0cd3641160c5af2acfeffffff0350da1100000000001976a9149fe7cf03a32c8aa2dd684cb6ac697dc6495c6a7d88ac17131900000000001976a9142aa576137000b3e6e1ecb319657663df1a24982588acc0354e02000000001976a91400327f03b93172a7df917639cc5fb3e01822e93d88ac9c3b1800")
        result: BroadcastResult = await client_manager.broadcast_transaction_and_listen(rawtx,
            broadcast_peer_count=1)
        if result.rejected is not None:
            logger.error(f"Transaction rejected. Reason: {result.rejected}")
        else:
            logger.error(f"Transaction accepted. Relaying peer_ids: {result.relaying_peer_ids}")

        # This get_headers method requires use_headers_queue=True
        client = await client_manager.get_next_available_peer()
        message = await client.get_headers(
            hash_count=1,
            block_locator_hashes=[ZERO_HASH],
            hash_stop=ZERO_HASH
        )
        if message:
            headers: list[BlockHeader] = client.deserializer.headers(BytesIO(message))
            for header in headers:
                logger.debug(f"Got header. Hash: {hash_to_hex_str(header.hash)}")

        logger.debug(f"Waiting for the new chain tip")
        while True:
            inv_vect = await client.inv_queue.get()
            logger.debug(f"Got inv_vect: {inv_vect}")

asyncio.run(main())
