from jaqsmds.server.repliers.utils import DBHandler
import pandas as pd
from datetime import datetime, time


DATETIME = "datetime"
ID = "_id"
D = "_d"
L = "_l"
FREQ = "freq"


RESAMPLE_MAP = {
    "high": "max",
    "low": "min",
    "open": "first",
    "close": "last",
    "volume": "sum",
    "turnover": "sum"
}


def get_reample_map(columns):
    return {name: RESAMPLE_MAP[name] for name in columns }


SHORT2LONG = {
    "SH": "XSHG",
    "SZ": "XSHE"
}


def time_tup(num):
    a = int(num/10000)
    b = int((num-a*10000)/100)
    c = num % 100
    return a, b, c


def split_dt(dt):
    return dt.year*10000+dt.month*100+dt.day, dt.hour*10000+dt.minute*100+dt.second


def transfer_symbol(name):
    code, short_ex = name.split(".", 1)
    long_ex = SHORT2LONG[short_ex]
    return code, ".".join([code, long_ex])


class JsiHandler(DBHandler):

    def __init__(self, client, db):
        super(JsiHandler, self).__init__(client, db, "jsi")

    def receive(self, **kwargs):
        result = super(JsiHandler, self).receive(**kwargs)
        result[FREQ] = kwargs.get(FREQ, "1M").upper()
        fields = kwargs.get("fields", "")
        if "vwap" in fields or (fields == ""):
            result["vwap"] = result["turnover"]/result["volume"]
        return result

    def adapt(self, symbol, begin_time=90000, end_time=160000, trade_date=0, freq="1M", fields=""):
        if begin_time > end_time:
            begin_time = 90000
            end_time = 160000

        if fields != "":
            prj = dict.fromkeys(fields.split(","), 1)
            prj[DATETIME] = 1
            prj[ID] = 0
            prj[D] = 1
            prj[L] = 1
        else:
            prj = {ID: 0}

        if "vwap" in prj:
            prj["turnover"] = 1
            prj["volume"] = 1

        return symbol.split(","), \
               (trade_date, time(*time_tup(begin_time)), time(*time_tup(end_time)), prj, freq.upper()), {}

    def read_doc(self, name, trade_date, projection):
        if trade_date == 0:
            return self.db[name].find_one(projection=projection, sort=[("_d", -1)])
        else:
            d = datetime(*time_tup(trade_date))
            return self.db[name].find_one({"_d": d}, projection)

    def read_one(self, name, trade_date, start, end, projection, freq):
        code, col_name = transfer_symbol(name)
        result = self.read_doc(col_name, trade_date, projection)
        date = result.pop(D, None)
        result.pop(L)
        data = pd.DataFrame(result).set_index(DATETIME)
        if freq != "1M":
            data = data.resample(
                freq.replace("M", "min"), closed="right", label="right"
            ).apply(get_reample_map(data.columns)).dropna()
        data[['date', "time"]] = pd.DataFrame(list(map(split_dt, data.index)), data.index)
        data["trade_date"] = data["date"]
        data["code"] = code
        data["symbol"] = name
        s = date.replace(hour=start.hour, minute=start.minute, second=start.second)
        e = date.replace(hour=end.hour, minute=end.minute, second=end.second)
        return data.loc[s:e]