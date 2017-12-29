# encoding:utf-8
from pymongo import MongoClient
from jaqs.data import DataApi
from jaqs.data.dataapi.jrpc_py import _pack_msgpack_snappy, _unpack_msgpack_snappy
from jaqs.data.dataapi.utils import extract_result
from jaqsmds.collector.structure import *
from fxdayu_data.handler.mongo_handler import write, read
import logging


def view_pointer(client, qs):
    db, collection = qs.view.split(".", 1)
    return client[db][collection]


def direct_pull(client, api, qs, *args, **kwargs):
    collection = view_pointer(client, qs)
    data, msg = api.query(**qs.query(*args, **kwargs))
    if msg == "0,":
        result = write(collection, data)
        return result
    else:
        raise ValueError(msg)


def get(api, query):
    df, msg = api.query(**query)
    if msg == "0,":
        return df
    else:
        raise ValueError(msg)


def current_instrument(collection):
    return read(collection, None,
                filter={"inst_type": "1", "status": "1", "$or": [{"market": "SZ"}, {"market": "SH"}]},
                projection=["symbol"])["symbol"]


def timer(func):
    from datetime import datetime

    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        logging.warning(datetime.now()-start)
        return result
    return wrapper

import pandas as pd

@timer
def fetch_db(col):
    # return pd.DataFrame(list(col.find({"inst_type": "1", "status": "1", "$or": [{"market": "SZ"}, {"market": "SH"}]})))
    data = pd.DataFrame(list(col.find({"inst_type": "1", "status": "1", "$or": [{"market": "SZ"}, {"market": "SH"}]}, projection={"_id": 0})))
    # dct = {name: value.tolist() for name, value in data.items()}
    # packed = _pack_msgpack_snappy(dct)
    # reload = _unpack_msgpack_snappy(packed)
    # data = extract_result(reload, "pandas")
    return data

@timer
def fetch_remote(api):
    # return api.query(**InstrumentInfo(inst_type=1, status=1, market="SH,SZ"))
    return api.query(**InstrumentInfo())


if __name__ == '__main__':
    client = MongoClient("192.168.0.102,192.168.0.101")

    data = read(client.jz.instrumentInfo, None)
    res = data.groupby(data.inst_type)["symbol"].agg(lambda s: len(s))
    print(res)
    # result=data.groupby(data.market)["name"].agg(lambda s: len(s))
    # print(result)
    # symbols = current_instrument(client.jz.instrumentInfo)
    # api = DataApi()
    # api.login("13823156147", "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTI3ODY3ODYxODMiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM4MjMxNTYxNDcifQ.Lt4orfuPoP5xVM_t3n4SdC7xwPNDoloHdvCAWU4JfYQ")
    # data = api.query(**MFPortfolio(symbol="161911.SZ"))
    # print(*data, sep="\n")
    # direct_pull(client, api, IndexCons, index_code=symbols, 1start_date=0, end_date=99999999)
    #

