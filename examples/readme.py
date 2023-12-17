import asyncio
from io import BytesIO
import logging

from bitcoinx import hash_to_hex_str

from conduit_flat_file_db import BitcoinClient, REGTEST, HandlersDefault, BitcoinClientManager, BitcoinClientMode
from conduit_flat_file_db.constants import ZERO_HASH
from conduit_flat_file_db.types import BlockHeader

logging.basicConfig(format="%(asctime)-25s %(levelname)-10s %(name)-28s %(message)s")


class SPVApplicationHandlers(HandlersDefault):
    """Override the other callbacks as you need to."""

    def __init__(self, network_type: str):
        super().__init__(network_type)
        self.logger.setLevel(logging.DEBUG)  # To silence the logs from the default handlers, set a higher level

    async def on_headers(self, message: bytes, peer: BitcoinClient) -> None:
        if message[0:1] != b"\x00":  # Count of headers was zero
            headers: list[BlockHeader] = peer.deserializer.headers(BytesIO(message))
            for header in headers:
                self.logger.debug(f"Got header. Hash: {hash_to_hex_str(header.hash)}")

    async def on_inv(self, message: bytes, peer: BitcoinClient) -> None:
        inv_vect = peer.deserializer.inv(BytesIO(message))
        self.logger.debug("Received inv: %s (peer_id=%s)", inv_vect, peer.id)

    async def on_reject(self, message: bytes, peer: BitcoinClient) -> None:
        reject_msg = peer.deserializer.reject(BytesIO(message))
        self.logger.debug("Received reject: %s (peer_id=%s)", reject_msg, peer.id)


async def main():
    message_handler = SPVApplicationHandlers(REGTEST)
    peers_list = ["127.0.0.1:18444", "127.0.0.1:18444", "127.0.0.1:18444"]
    async with BitcoinClientManager(message_handler, peers_list, mode=BitcoinClientMode.ADVANCED) as client_manager:
        rawtx: bytes = b"raw transaction goes here"
        client = await client_manager.get_next_available_peer()
        message = client.serializer.tx(rawtx)
        client.send_message(message)

        message = client.serializer.getheaders(
            hash_count=1,
            block_locator_hashes=[ZERO_HASH],
            hash_stop=ZERO_HASH
        )
        client.send_message(message)
        await client.listen()


asyncio.run(main())
