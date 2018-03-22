from jaqsmds.server.repliers.utils import fill_field_filter, DBHandler, expand, date2int
from datetime import datetime
import pandas as pd
import logging


def timer(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        r = func(*args, **kwargs)
        print(datetime.now()-start)
        return r
    return wrapper


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


class DailyHandler(DBHandler):

    def __init__(self, client, db, adj=None, trade_cal='jz.secTradeCal'):
        super(DailyHandler, self).__init__(client, db, "daily")
        self.adj = adj
        self.empty = {}
        self._trade_cal_info = trade_cal
        self._trade_cal = None

    def get_trade_cal(self, start, end):
        if self._trade_cal:
            return self._get_trade_cal(start, end)
        else:
            self.init_trade_cal()
            return self._get_trade_cal(start, end)

    def _get_trade_cal(self, start, end):
        sliced = slice(*self._trade_cal.slice_locs(start, end, kind="loc"))
        return self._trade_cal[sliced]

    def init_trade_cal(self):
        db, name = self._trade_cal_info.split(".", 1)
        col = self.client[db][name]
        trade_cal = pd.DataFrame(
            list(col.find(None, {"istradeday": 1, "trade_date": 1, "_id": 0}))
        ).set_index("trade_date")
        self._trade_cal = trade_cal[trade_cal["istradeday"]=="T"].sort_index().index

    def receive(self, symbol, begin_date, end_date, **kwargs):
        data = super(DailyHandler, self).receive(symbol=symbol,
                                                 begin_date=begin_date,
                                                 end_date=end_date,
                                                 **kwargs)
        mode = kwargs.get("adjust_mode", "none")
        if mode == "none":
            return data
        else:
            adj = self.adj.parse(
                "symbol=%s&start_date=%s&end_date=%s" % (symbol, begin_date, end_date),
                ""
            )
            if len(adj):
                adj["trade_date"] = adj["trade_date"].apply(int)
                return adjust(data, adj, mode)
            else:
                return data

    def adapt(self, symbol, begin_date, end_date, fields="", adjust_mode="none", freq="1d"):
        symbol, begin_date, end_date = check(symbol, begin_date, end_date)
        symbols = symbol.replace(" ", "").split(",")
        f = {"datetime": {"$gte": begin_date, "$lte": end_date}}
        p = fill_field_filter(fields, "datetime")
        if "vwap" in p:
            p["volume"] = 1
            p["turnover"] = 1
        trade_days = self.get_trade_cal(date2int(begin_date), date2int(end_date))
        return symbols, (f, p, freq, trade_days), self.empty

    def read_one(self, symbol, f, p, freq, trade_days, **kwargs):
        code = symbol[:6]
        col = self.db[expand(symbol)]
        data = pd.DataFrame(list(col.find(f, p)))
        data.sort_index(inplace=True)
        if len(data) == 0:
            return data
        data = data.set_index("datetime").rename_axis(date2int)
        if "vwap" in p or (len(p) <= 1):
            try:
                data["vwap"] = (data["turnover"] / data["volume"]).round(2)
            except Exception as e:
                logging.error("daily| %s | %s", symbol, e)
        data["trade_status"] = "交易"
        data = pd.DataFrame(data, trade_days)
        data["trade_status"].fillna("停牌", inplace=True)
        fill_suspend(data)
        data["symbol"] = symbol
        data["code"] = code
        data["freq"] = freq
        data['trade_date'] = data.index
        return data


def fill_suspend(data):
    for name in ["turnover", "volume", "high", "low", "open", "vwap"]:
        if name in data.columns:
            data[name].fillna(0, inplace=True)

    if "close" in data.columns:
        data["close"].ffill(inplace=True)
        data["close"].bfill(inplace=True)

    return data

def reset_adj(adj, index):
    try:
        return pd.Series(adj.set_index(["symbol", "trade_date"])["adjust_factor"], index, dtype=float)
    except:
        return pd.Series(
            adj.drop_duplicates(["symbol", "trade_date"]).set_index(["symbol", "trade_date"])["adjust_factor"],
            index, dtype=float
        )


def adjust(price, adj, mode):
    if isinstance(price, pd.DataFrame) and isinstance(adj, pd.DataFrame):
        index = pd.MultiIndex.from_arrays([price['symbol'].values, price["trade_date"].values],
                                          names=["symbol", "trade_date"])
        factor = reset_adj(adj, index)
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
