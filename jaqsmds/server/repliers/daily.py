from jaqsmds.server.repliers.utils import fill_field_filter, date2str, DBReader, expand
from datetime import datetime
import pandas as pd
import logging


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

    def __init__(self, client, db, adj=None):
        super(DailyReader, self).__init__(client, db, "daily")
        self.adj = adj
        self.empty = {}

    def receive(self, symbol, begin_date, end_date, **kwargs):
        data = super(DailyReader, self).receive(symbol=symbol, begin_date=begin_date, end_date=end_date, **kwargs)
        mode = kwargs.get("adjust_mode", "none")
        if mode == "none":
            return data
        else:
            adj = self.adj.read(
                "symbol=%s&start_date=%s&end_date=%s" % (symbol, begin_date, end_date),
                ""
            )
            return adjust(data, adj, mode)

    def adapt(self, symbol, begin_date, end_date, fields="", adjust_mode="none", freq="1d"):
        symbol, begin_date, end_date = check(symbol, begin_date, end_date)
        symbols = symbol.replace(" ", "").split(",")
        f = {"datetime": {"$gte": begin_date, "$lte": end_date}}
        p = fill_field_filter(fields, "datetime")
        if "vwap" in p:
            p["volume"] = 1
            p["turnover"] = 1
        return symbols, (f, p, freq), self.empty

    def read_one(self, symbol, f, p, freq, **kwargs):
        code = symbol[:6]
        col = self.db[expand(symbol)]
        data = pd.DataFrame(list(col.find(f, p)))
        if len(data) == 0:
            return data
        data["trade_date"] = data.pop("datetime").apply(date2str)
        data["freq"] = freq
        if "vwap" in p or (len(p) <= 1):
            try:
                data["vwap"] = (data["turnover"] / data["volume"]).round(2)
            except Exception as e:
                logging.error("daily| %s | %s", symbol, e)
        data["trade_status"] = "交易"
        data["symbol"] = symbol
        data["code"] = code
        return data


def adjust(price, adj, mode):
    if isinstance(price, pd.DataFrame) and isinstance(adj, pd.DataFrame):
        index = pd.MultiIndex.from_arrays([price['symbol'].values, price["trade_date"].values],
                                          names=["symbol", "trade_date"])
        factor = pd.Series(adj.set_index(["symbol", "trade_date"])["adjust_factor"], index, dtype=float)
        if mode == "post":
            for name in factor.index.levels[0]:
                factor.loc[name] = factor.loc[name].ffill().bfill().values
        else:
            for name in factor.index.levels[0]:
                s = factor.loc[name].ffill().bfill()
                factor.loc[name] = (s / s.iloc[-1]).values

        af = pd.Series(factor.values)
        for name in PRICE:
            if name in price:
                price[name] = (price[name] * af).round(2)
        return price
