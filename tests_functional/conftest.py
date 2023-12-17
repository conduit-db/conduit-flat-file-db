# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import json
import logging

import requests


logger = logging.getLogger("call-node-rpc")


def call_any(method_name: str, *args, rpcport: int=18332, rpchost: str="127.0.0.1", rpcuser:
        str="rpcuser", rpcpassword: str="rpcpassword"):
    """Send an RPC request to the specified bitcoin node"""
    result = None
    try:
        if not args:
            params = []
        else:
            params = [*args]
        payload = json.dumps(
            {"jsonrpc": "2.0", "method": f"{method_name}", "params": params, "id": 0})
        result = requests.post(f"http://{rpcuser}:{rpcpassword}@{rpchost}:{rpcport}", data=payload,
                               timeout=10.0)
        result.raise_for_status()
        return result
    except requests.exceptions.HTTPError as e:
        if result is not None:
            logger.error(result.json()['error']['message'])
        raise e
