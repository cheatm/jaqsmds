from jaqsmds.server.repliers.jset import iter_filter, split_conditions, field_filter
from datetime import datetime
import pandas as pd
import logging


def time_range(start=None, end=None):
    dct = {}
    if start:
        dct['$gte'] = datetime.strptime(start, "%Y%m%d").replace(hour=15)
    if end:
        dct["%lte"] = datetime.strptime(end, "%Y%m%d").replace(hour=15)
    return dct


def time2str(t):
    return t.strftime("%Y%m%d")


class FactorReader(object):

    def __init__(self, db):
        self.db = db

    def iter_read(self, codes, f, p):
        for code in codes:
            try:
                yield self._read(code, f.copy(), p.copy())
            except Exception as e:
                logging.error("factor | %s | %s", code, e)

    def read(self, filter, fields):
        codes, f = self.split_filter(filter)
        projection = field_filter(fields)
        if len(projection) > 1:
            projection["datetime"] = 1
        data = pd.concat(list(self.iter_read(codes, f, projection)))
        return {name: item.tolist() for name, item in data.items()}

    def _read(self, name, f, p):
        data = pd.DataFrame(list(self.db[name].find(f, p)))
        try:
            data['datetime'] = data["datetime"].apply(time2str)
        except:
            pass
        return data

    def split_filter(self, string):
        dct = dict(iter_filter(string))
        codes = dct.pop("symbol")
        f = dict(self.split_range(dct))
        conditions = list(split_conditions(dct))
        if len(conditions) == 1:
            dct.update(conditions[0])
        elif len(conditions) > 1:
            dct["$and"] = conditions
        dct.update(f)
        return codes.split(","), dct

    @staticmethod
    def split_range(dct):
        r = time_range(dct.pop("start", None), dct.pop("end", None))
        if len(r):
            return {"datetime": r}
        else:
            return {}
