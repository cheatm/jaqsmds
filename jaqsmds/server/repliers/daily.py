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


class DailyReader(DBReader):

    def __init__(self, client, db):
        super(DailyReader, self).__init__(client, db, "daily")
        self.empty = {}

    def adapt(self, symbol, begin_date, end_date, fields="", adjust_mode="none", freq="1d"):
        symbol, begin_date, end_date = check(symbol, begin_date, end_date)
        symbols = symbol.replace(" ", "").split(",")
        f = {"datetime": {"$gte": begin_date, "$lte": end_date}}
        p = fill_field_filter(fields, "datetime")
        return symbols, (f, p, freq), self.empty

    def read_one(self, symbol, f, p, freq, **kwargs):
        code = symbol[:6]
        col = self.db[expand(code)]
        data = pd.DataFrame(list(col.find(f, p)))
        data["datetime"] = data["datetime"].apply(date2str)
        data["freq"] = freq
        data["vwap"] = (data["turnover"] / data["volume"]).round(2)
        data["trade_status"] = "交易"
        data["symbol"] = symbol
        data["code"] = code
        return data
