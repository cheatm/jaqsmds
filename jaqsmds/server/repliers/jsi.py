from jaqsmds.server.repliers.utils import DBHandler, MongodbHandler
import pandas as pd
from datetime import datetime, time
import logging


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


FUTURE_MULTIPLIERS = {
    'IF': 300, 'IH': 300, 'IC': 200, 'TF': 10000, 'T': 10000, 'CY': 5, 'ER': 10, 'WT': 10, 'RO': 5, 'AP': 10, 'WS': 10,
    'ME': 50, 'LR': 20, 'JR': 20, 'TC': 200, 'WH': 20, 'RS': 10, 'FG': 20, 'CF': 5, 'MA': 10, 'TA': 5, 'PM': 50,
    'RI': 20, 'OI': 10, 'SM': 5, 'SR': 10, 'ZC': 100, 'SF': 5, 'RM': 10, 'jm': 60, 'b': 10, 'v': 5, 'm': 10, 'l': 5,
    'c': 10, 'y': 10, 'j': 100, 'bb': 500, 'pp': 5, 'p': 10, 'jd': 10, 'fb': 500, 'cs': 10, 'a': 10, 'i': 100,
    'sc': 1000, 'fu': 50, 'hc': 10, 'au': 1000, 'wr': 10, 'zn': 5, 'al': 5, 'bu': 10, 'rb': 10, 'sn': 1, 'cu': 5,
    'ni': 1, 'ru': 10, 'ag': 15, 'pb': 5
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

    def __init__(self, client, db, multipliers=None):
        super(JsiHandler, self).__init__(client, db, "jsi")
        self.multipliers = {}
        if isinstance(multipliers, dict):
            self.multipliers.update(multipliers)

    def receive(self, **kwargs):
        result = super(JsiHandler, self).receive(**kwargs)
        result[FREQ] = kwargs.get(FREQ, "1M").upper()
        return result

    def adapt(self, symbol, begin_time=210000, end_time=160000, trade_date=0, freq="1M", fields=""):
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

    @staticmethod
    def _loc_time(index, t):
        if t >= time(20):
            if index[-1].date() > index[0].date():
                return index[0].replace(hour=t.hour, minute=t.minute, second=t.second)
            else:
                return index[0]
        else:
            return index[-1].replace(hour=t.hour, minute=t.minute, second=t.second)

    def read_one(self, name, trade_date, start, end, projection, freq):
        data = self._read(name, trade_date, projection, freq)
        s = self._loc_time(data.index, start)
        e = self._loc_time(data.index, end)
        data = data.loc[s:e]
        data[['date', "time"]] = pd.DataFrame(list(map(split_dt, data.index)), data.index)
        data["trade_date"] = data["date"]
        data["code"] = name.split(".", 1)[0]
        data["symbol"] = name
        if "vwap" in projection or (len(projection) == 1):
            if name in self.multipliers:
                data["vwap"] = data["turnover"]/data["volume"]/self.multipliers[name]
            else:
                data["vwap"] = data["turnover"]/data["volume"]
        return data

    def _read(self, name, trade_date, projection, freq):
        result = self.read_doc(name, trade_date, projection)
        result.pop(D, None)
        result.pop(L, None)
        data = pd.DataFrame(result).set_index(DATETIME)
        if freq != "1M":
            data = data.resample(
                freq.replace("M", "min"), closed="right", label="right"
            ).apply(get_reample_map(data.columns)).dropna()
        return data


class StockJsiHandler(JsiHandler):

    def read_one(self, name, trade_date, start, end, projection, freq):
        code, col_name = transfer_symbol(name)
        data = super(StockJsiHandler, self).read_one(col_name, trade_date, start, end, projection, freq)
        data["symbol"] = name
        return data


class _JsiHandler(DBHandler):

    def __init__(self, client, db):
        super(_JsiHandler, self).__init__(client, db, "jsi")

    def receive(self, **kwargs):
        result = super(_JsiHandler, self).receive(**kwargs)
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


class MultiJsiHandler(MongodbHandler):

    def __init__(self, client, stock, future):
        super(MultiJsiHandler, self).__init__(client)
        self.client = client
        self.stock = StockJsiHandler(self.client, stock)
        self.future = JsiHandler(self.client, future, FUTURE_MULTIPLIERS)
        self.classifier = {}
        self.classifier.update(dict.fromkeys(["CZC", "CFE", "DCE", "SHF"], 1))
        self.handlers = {0: self.stock,
                         1: self.future}

    def receive(self, symbol, **kwargs):
        dct = {0: [], 1: []}
        for s in symbol.split(","):
            dct[self.distinguish(s)].append(s)
        results = []
        for key, symbols in dct.items():
            if len(symbols):
                try:
                    results.append(self.handlers[key].receive(symbol=','.join(symbols), **kwargs))
                except Exception as e:
                    logging.error("read jsi part | %s | %s | %s", key, symbols, e)

        if len(results) == 1:
            return results[0]
        elif len(results) > 1:
            return pd.concat(results, ignore_index=True)
        else:
            return pd.DataFrame()

    def distinguish(self, symbol):
        code, ex = symbol.split(".", 1)
        return self.classifier.get(ex, 0)
