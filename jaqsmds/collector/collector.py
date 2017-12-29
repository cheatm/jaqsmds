# encoding:utf-8
from jaqsmds.collector.table import *
import logging
from collections import Iterable
import pandas as pd


def read(collection, filter=None, projection=None, **kwargs):
    if not isinstance(projection, dict):
        if isinstance(projection, Iterable):
            projection = dict.fromkeys(projection, 1)
        else:
            projection = {}

    projection["_id"] = 0

    return pd.DataFrame(list(collection.find(filter, projection, **kwargs)))


def write(collection, data, index=None):
    if index:
        if index not in data.columns:
            data = pd.DataFrame(data)
            data[index] = data.index

    return collection.insert_many([s.to_dict() for name, s in data.iterrows()])


def write_2d(api, client, table2d, **kwargs):
    qs = table2d.qs
    db, col = table2d.db.split(".", 1)
    df, msg = api.query(**qs(**kwargs))
    if msg == "0,":
        collection = client[db][col]
        result = write(collection, df)
        for idx in table2d.index:
            collection.create_index(idx, background=True)
        return result
    else:
        raise ValueError(msg)


def write_2d_iter(api, client, table2d, name, values, **kwargs):
    for value in values:
        dct = kwargs.copy()
        dct[name] = value
        try:
            result = write_2d(api, client, table2d, **dct)
        except Exception as e:
            logging.error("%s | %s", table2d.qs.view, e)
        else:
            logging.warning("%s | %s", table2d.qs.view, result.acknowledged)


def write_3d_one(api, db, table3d, name, value, **kwargs):
    qs = table3d.qs
    kwargs[name] = value
    df, msg = api.query(**qs(**kwargs))
    if msg == "0,":
        collection = db[value]
        result = write(collection, df)
        for idx in table3d.index:
            collection.create_index(idx, background=True)
        return result
    else:
        raise ValueError(msg)


def write_3d_all(api, client, table3d, name, values, **kwargs):
    db = client[table3d.db]
    for value in values:
        try:
            result = write_3d_one(api, db, table3d, name, value, **kwargs)
        except Exception as e:
            logging.error("%s | %s | %s", table3d.db, value, e)
        else:
            logging.warning("%s | %s | %s", table3d.db, value, result.acknowledged)


def write_2d_all(api, client, tables, **kwargs):
    for table in tables:
        try:
            result = write_2d(api, client, table, **kwargs)
        except Exception as e:
            logging.error("%s | %s", table.qs.view, e)
        else:
            logging.warning("%s | %s", table.qs.view, result.acknowledged)


def read_inst_symbols(collection, _filter):
    return read(collection, _filter, projection={"_id": 0, "symbol": 1})["symbol"]


def stock_symbols(collection):
    return read_inst_symbols(
        collection,
        {"inst_type": "1", "$or": [{"market": "SZ"}, {"market": "SH"}], "status": "1"}
    )


def index_symbols(collection):
    return read_inst_symbols(
        collection,
        {"inst_type": "100", "$or": [{"market": "SZ"}, {"market": "SH"}], "status": "1"}
    )


def mf_symbols(collection):
    return read_inst_symbols(
        collection,
        {"inst_type": "3", "$or": [{"market": "SZ"}, {"market": "SH"}], "status": "1"}
    )



INDIVIDUALS = [InstrumentInfoTable, SecTradeCalTable, SecSuspTable, SecRestrictedTable]
SYMBOL2D = [SecDividendTable, SecIndustryTable, ProfitExpressTable]
INDEX2D = [IndexConsTable]
SYMBOL3D = [SecAdjFactorTable, SecDailyIndicatorTable, BalanceSheetTable, IncomeTable, CashFlowTable]
MF3D = [MFNavTable, MFPortfolioTable, MFDividendTable, MFBondPortfolioTable]


def main():
    from pymongo import MongoClient
    from jaqs.data import DataApi
    from jaqsmds.collector.structure import MFNav, MFPortfolio, MFDividend, MFBondPortfolio

    client = MongoClient(port=37017)
    indexes = index_symbols(client.jz.instrumentInfo)
    # mf_symbol = mf_symbols(client.jz.instrumentInfo)

    api = DataApi()
    api.login("13823156147", "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTI3ODY3ODYxODMiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM4MjMxNTYxNDcifQ.Lt4orfuPoP5xVM_t3n4SdC7xwPNDoloHdvCAWU4JfYQ")

    write_2d_iter(api, client, IndexConsTable, "index_code", indexes)
    # write_2d_all(api, client, INDEX2D, index_code=",".join(indexes))

    # for table in MF3D:
    #     write_3d_all(
    #         api,
    #         client,
    #         table,
    #         "symbol",
    #         mf_symbol
    #     )

if __name__ == '__main__':
    main()