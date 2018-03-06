from jaqsmds.server.repliers.utils import Jset3DReader, fill_field_filter, date2int, time_range_daily, \
    QueryInterpreter, DBReader
from datetime import datetime


def str2date(string):
    try:
        return datetime.strptime(string.replace("-", ""), "%Y%m%d").replace(hour=15)
    except:
        return None


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
