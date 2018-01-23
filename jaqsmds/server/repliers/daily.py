from jaqsmds.server.repliers.utils import fill_field_filter, date2str, DBReader, expand
from datetime import datetime
import pandas as pd


def check(symbol, begin_date, end_date):
    if len(symbol) == 0:
        raise ValueError("symbol is invalid")

    if 0 < begin_date:
        begin_date = datetime.strptime(str(begin_date), "%Y%m%d").replace(hour=15)
    else:
        begin_date = datetime(1, 1, 1)

    if 20201231 >= end_date and (0 < end_date):
        end_date = datetime.strptime(str(end_date), "%Y%m%d").replace(hour=15)
    else:
        end_date = datetime(2020, 12, 31)

    return symbol, begin_date, end_date


PRICE = ["open", "high", "low", "close", "vwap"]


class DailyReader(DBReader):

    def __init__(self, client, db, adj="lb.secAdjFactor"):
        super(DailyReader, self).__init__(client, db, "daily")
        adjs = adj.split(".")
        self.adj = self.client[adjs[0]][adjs[1]]
        self.empty = {}

    def adapt(self, symbol, begin_date, end_date, fields="", adjust_mode="none", freq="1d"):
        symbol, begin_date, end_date = check(symbol, begin_date, end_date)
        symbols = symbol.replace(" ", "").split(",")
        f = {"datetime": {"$gte": begin_date, "$lte": end_date}}
        p = fill_field_filter(fields, "datetime")
        if "vwap" in p:
            p["volume"] = 1
            p["turnover"] = 1
        return symbols, (f, p, freq, adjust_mode), self.empty

    def read_one(self, symbol, f, p, freq, adj_mode, **kwargs):
        code = symbol[:6]
        col = self.db[expand(code)]
        data = pd.DataFrame(list(col.find(f, p)))
        data["trade_date"] = data.pop("datetime").apply(date2str)
        data["freq"] = freq
        if "vwap" in p:
            data["vwap"] = (data["turnover"] / data["volume"]).round(2)
        if adj_mode != "none":
            data = self.adj_data(symbol, adj_mode, data)
        data["trade_status"] = "交易"
        data["symbol"] = symbol
        data["code"] = code
        return data

    def adj_data(self, symbol, mode, data):
        f = {"trade_date": {"$gte": data["trade_date"][0], "$lte": data.iloc[-1]["trade_date"]},
             "symbol": symbol}
        p = {"adjust_factor": 1, "_id": 0, "trade_date": 1}
        adj = pd.DataFrame(list(self.adj.find(f, p)))
        if len(adj):
            adj = adj.set_index("trade_date").applymap(float)
        else:
            return data
        if mode != "post":
            adj = adj.apply(lambda s: s/adj.iloc[-1]["adjust_factor"])
        new = data.set_index("trade_date", drop=False)
        new["adj"] = adj["adjust_factor"]
        new["adj"] = new["adj"].ffill().bfill()
        for p in PRICE:
            if p in new.columns:
                new[p] = (new[p] * new["adj"]).round(2)
        new.pop("adj")
        return new