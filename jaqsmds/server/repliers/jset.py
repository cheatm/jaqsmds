import pandas as pd
from functools import partial
import logging


def iter_filter(string):
    s = string.replace(" ", "")
    if s == "":
        return
    for pair in s.split("&"):
        key, value = pair.split("=")
        if len(value):
            yield key, value


def field_filter(string):
    dct = {s: 1 for s in string.replace(" ", "").split(",")}
    dct["_id"] = 0
    return dct


def split_conditions(dct):
    for key, value in list(dct.items()):
        if ',' in value:
            del dct[key]
            yield {"$or": [{key: v} for v in value]}


class JsetReader2D(object):

    def __init__(self, collection, ranges=None, view=None):
        self.view = view
        self.collection = collection
        self.ranges = ranges if isinstance(ranges, dict) else {}

    def read(self, filter, fields):
        data = pd.DataFrame(list(self.collection.find(self.split_filter(filter), field_filter(fields))))
        return {name: item.tolist() for name, item in data.items()}

    def split_filter(self, string):
        dct = dict(iter_filter(string))
        f = dict(self.split_range(dct))
        conditions = list(split_conditions(dct))
        if len(conditions) == 1:
            dct.update(conditions[0])
        elif len(conditions) > 1:
            dct["$and"] = conditions
        dct.update(f)
        return dct

    def split_range(self, dct):
        for key, value in self.ranges.items():
            r = {}
            start = dct.pop("start_%s" % key, None)
            if start:
                r["$gte"] = start
            end = dct.get("end_%s" % key, None)
            if end:
                r["$lte"] = end
            if len(r):
                yield value, r


class JsetReader3d(object):

    def __init__(self, db, key, ranges=None, view=None):
        self.view = view
        self.db = db
        self.key = key
        self.ranges = ranges if isinstance(ranges, dict) else {}

    def read(self, filter, fields):
        codes, f = self.split_filter(filter)
        projection = field_filter(fields)
        data = pd.concat(list(self.iter_read(codes, f, projection)))
        return {name: item.tolist() for name, item in data.items()}

    def iter_read(self, codes, f, p):
        for code in codes:
            try:
                yield self._read(code, f.copy(), p.copy())
            except Exception as e:
                logging.error("%s | %s | %s", self.view, code, e)

    def _read(self, name, f, p):
        return pd.DataFrame(list(self.db[name].find(f, p)))

    def split_filter(self, string):
        dct = dict(iter_filter(string))
        codes = dct.pop(self.key)
        f = dict(self.split_range(dct))
        conditions = list(split_conditions(dct))
        if len(conditions) == 1:
            dct.update(conditions[0])
        elif len(conditions) > 1:
            dct["$and"] = conditions
        dct.update(f)
        return codes.replace(" ", "").split(","), dct

    def split_range(self, dct):
        for key, value in self.ranges.items():
            r = {}
            start = dct.pop("start_%s" % key, None)
            if start:
                r["$gte"] = start
            end = dct.get("end_%s" % key, None)
            if end:
                r["$lte"] = end
            if len(r):
                yield value, r


InstrumentInfoReader = partial(JsetReader2D)
SecTradeCalReader = partial(JsetReader2D, ranges={"date": "trade_date"})
SecDividendReader = partial(JsetReader2D, ranges={"date": "ann_date"})
SecSuspReader = partial(JsetReader2D, ranges={"date": "ann_date"})
SecIndustryReader = partial(JsetReader2D)
SecDailyIndicatorReader = partial(JsetReader2D, ranges={"date": "trade_date"})
BalanceSheetReader = partial(
    JsetReader2D,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
IncomeReader = partial(
    JsetReader2D,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
CashFlowReader = partial(
    JsetReader2D,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
ProfitExpressReader = partial(
    JsetReader2D,
    ranges={"anndate": "ann_date", "reportdate": "report_date"}
)
SecRestrictedReader = partial(JsetReader2D, ranges={"date": "list_date"})
IndexConsReader = partial(JsetReader2D, ranges={"date": "in_date"})


LB = {"lb.secDividend": SecDividendReader,
      "lb.secSusp": SecSuspReader,
      "lb.secIndustry": SecIndustryReader,
      "lb.secDailyIndicator": SecDailyIndicatorReader,
      "lb.balanceSheet": BalanceSheetReader,
      "lb.income": IncomeReader,
      "lb.cashFlow": CashFlowReader,
      "lb.profitExpress": ProfitExpressReader,
      "lb.secRestricted": SecRestrictedReader,
      "lb.indexCons": IndexConsReader}


def lb_readers(db):
    return {name: cls(db[name[3:]]) for name, cls in LB.items()}


JZ = {"jz.instrumentInfo": InstrumentInfoReader,
      "jz.secTradeCal": SecTradeCalReader}


def jz_readers(db):
    return {name: cls(db[name[3:]]) for name, cls in JZ.items()}

