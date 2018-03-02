from jaqsmds.server.repliers.utils import Jset3DReader, fill_field_filter, date2str, time_range_daily, \
    QueryInterpreter, DBReader


# class FactorReader(Jset3DReader):
#
#     def __init__(self, db):
#         super(FactorReader, self).__init__(db, view="factor")
#
#     @staticmethod
#     def field_filter(string):
#         return fill_field_filter(string, 'datetime')
#
#     def split_range(self, dct):
#         r = time_range_daily(dct.pop("start", None), dct.pop("end", None))
#         if len(r):
#             yield "datetime", r
#
#     def _read(self, symbol, f, p):
#         data = super(FactorReader, self)._read(symbol, f, p)
#         data["symbol"] = symbol
#         data["datetime"] = data["datetime"].apply(date2str)
#         return data


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
        data["datetime"] = data["datetime"].apply(date2str)
        return data


if __name__ == '__main__':
    from pymongo import MongoClient
    result = FactorReader(
        MongoClient("192.168.0.102")["factor"]
    ).parse("symbol=000001.XSHE&start=20170101&end=99999999", 'LCAP,LFLO,ETP5,PE,PB,ROE')
    print(result)