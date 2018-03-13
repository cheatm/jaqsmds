from jaqsmds.server.repliers.utils import date2int, QueryInterpreter, DBReader, DailyAxisInterpreter, DailyAxisReader
from datetime import datetime


def str2date(string):
    try:
        return datetime.strptime(string.replace("-", ""), "%Y%m%d").replace(hour=15)
    except:
        return None


def timer(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        expiry = datetime.now() - start
        print("{}: {}".format(func.__name__, expiry))
        return result

    return wrapper


class FactorInterpreter(QueryInterpreter):

    def __init__(self):
        super(FactorInterpreter, self).__init__("factor", primary="symbol")

    def catch(self, dct):
        start = dct.pop("start", None)
        if start:
            start = str2date(start)
        end = dct.pop("end", None)
        if end:
            end = str2date(end)
        if start or end:
            yield "datetime", (start, end)

    def fields(self, string):
        result = super(FactorInterpreter, self).fields(string)
        if len(result) > 1:
            result["datetime"] = 1
        return result


class FactorReader(DBReader):

    def __init__(self, db):
        super(FactorReader, self).__init__(db, FactorInterpreter())

    def read(self, name, filters, projection):
        data = super(FactorReader, self).read(name, filters, projection)
        data["symbol"] = name
        data["datetime"] = data["datetime"].apply(date2int)
        return data


class DailyFactorInterpreter(DailyAxisInterpreter):

    def __init__(self):
        super(DailyFactorInterpreter, self).__init__("symbol", default="datetime")

    def catch(self, dct):
        yield self.default, (self._catch("start", dct), self._catch("end", dct))

    def _catch(self, name, dct):
        date = dct.pop(name, None)
        if date:
            return str2date(date)

    def axis2(self, a1):
        a2 = set([s[:6] for s in a1.pop(self.axis, [])])
        a2.add(self.default)
        return a2


def expand(code):
    if code.startswith("6"):
        return code + ".XSHG"
    else:
        return code + ".XSHE"


class DailyFactorReader(DailyAxisReader):

    def __init__(self, db):
        super(DailyFactorReader, self).__init__(db, DailyFactorInterpreter())

    def decorate(self, data):
        data = super(DailyFactorReader, self).decorate(data.rename_axis(expand, 2))
        data[self.index] = data[self.index].apply(date2int)
        return data