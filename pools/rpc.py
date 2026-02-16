# pylint: disable=broad-except
"""
╔╗ ╦╔╦╗╔═╗╦ ╦╔═╗╦═╗╔═╗╔═╗  ╔╗╔╔═╗╔╦╗╦ ╦╔═╗╦═╗╦╔═╔═╗
╠╩╗║ ║ ╚═╗╠═╣╠═╣╠╦╝║╣ ╚═╗  ║║║║╣  ║ ║║║║ ║╠╦╝╠╩╗╚═╗
╚═╝╩ ╩ ╚═╝╩ ╩╩ ╩╩╚═╚═╝╚═╝  ╝╚╝╚═╝ ╩ ╚╩╝╚═╝╩╚═╩ ╩╚═╝

LIQUIDITY POOL MAPPER
"""

# STANDARD PYTHON MODULES
import time
from json import dumps as json_dumps
from json import loads as json_loads
from random import shuffle
import json

# THIRD PARTY MODULES
from websocket import create_connection as wss
import requests

# LIQUIDITY POOL MAPPER MODULES
from config import NODES


def wss_handshake():
    """
    Create a websocket handshake
    """
    nodes = NODES[::]
    shuffle(nodes)
    while True:
        try:
            nodes.append(nodes.pop(0))
            node = nodes[0]
            start = time.time()
            rpc = wss(node, timeout=3)
            if time.time() - start < 3:
                break
        except Exception as e:
            print(e)
    print(f"Successfully connected to {node}!")
    return rpc


def wss_query(rpc, params):
    """
    Send and receive websocket requests
    """
    query = json_dumps({"method": "call", "params": params, "jsonrpc": "2.0", "id": 1})
    rpc.send(query)
    ret = json_loads(rpc.recv())
    try:
        ret = ret["result"]  # if there is result key take it
    except Exception:
        print(ret)
    return ret


def rpc_get_objects(rpc, object_ids):
    """
    Return data about objects in 1.7.x, 2.4.x, 1.3.x, etc. format
    """
    ret = wss_query(rpc, ["database", "get_objects", [object_ids]])
    return {object_ids[idx]: item for idx, item in enumerate(ret) if item is not None}


def rpc_ticker(rpc, pair):
    """
    RPC the latest ticker price
    ~
    :RPC param base: symbol name or ID of the base asset
    :RPC param quote: symbol name or ID of the quote asset
    :RPC returns: The market ticker for the past 24 hours
    """
    asset, currency = pair.split(":")
    ticker = wss_query(rpc, ["database", "get_ticker", [currency, asset, False]])
    return float(ticker["latest"])


def get_max_object(_, space="1.19."):
    """
    Get the newest object id of the specified type (pool or other)
    using Elasticsearch query.
    """
    ELASTICSEARCH_URL = "https://es.bitshares.dev/bitshares-*/_search"

    field = (
        "operation_history.operation_result_object.data_object.new_objects"
        if type == "pool"
        else "operation_history.operation_result_object.data_string"
    )
    query = {
        "track_total_hits": True,
        "sort": [{"block_data.block_time": {"order": "desc"}}],
        "fields": [{"field": field}],
        "size": 1,
        "_source": False,
        "query": {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "match": {
                                        "operation_type": "59" if space == "1.19." else "10"
                                    }
                                }
                            ],
                            "minimum_should_match": 1,
                        }
                    },
                    {"exists": {"field": "operation_history.operation_result"}},
                ]
            }
        },
    }

    try:
        response = requests.post(
            ELASTICSEARCH_URL,
            headers={"Content-Type": "application/json"},
            data=json.dumps(query),
        )
        response.raise_for_status()
        data = response.json()
        latest = data["hits"]["hits"][0]["fields"][field][0]
        return int(latest.split(".")[2])
    except Exception as e:
        raise Exception(f"Failed to fetch from Elasticsearch: {e}")


def get_liquidity_pool_volume(rpc, pools):
    """
    get the sum amount a volume for a given set of liquidity pools
    """
    return {
        i["id"]: int(i["statistics"]["_24h_exchange_a2b_amount_a"])
        + int(i["statistics"]["_24h_exchange_b2a_amount_a"])
        for i in wss_query(
            rpc, ["database", "get_liquidity_pools", [pools, False, True]]
        )
    }


def rpc_get_feed(rpc, data_id):
    """
    return the oracle feed price for a given MPA
    """
    # given the bitasset_data_id, get the median feed price
    feed = rpc_get_objects(rpc, [data_id])[data_id]["median_feed"]["settlement_price"]
    # calculate the base feed amount in human terms
    base = int(feed["base"]["amount"])
    base_asset_id = feed["base"]["asset_id"]
    base_precision = int(
        rpc_get_objects(rpc, [base_asset_id])[base_asset_id]["precision"]
    )
    # calculate the quote feed amount in human terms
    quote = int(feed["quote"]["amount"])
    quote_asset_id = feed["quote"]["asset_id"]
    quote_precision = int(
        rpc_get_objects(rpc, [quote_asset_id])[quote_asset_id]["precision"]
    )
    # convert fractional human price to floating point
    return (base / 10**base_precision) / (quote / 10**quote_precision)
