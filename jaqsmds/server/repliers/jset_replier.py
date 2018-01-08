from jaqsmds.collector.table import *
from jaqsmds.server.repliers.db_replier import MongodbHandler, read
import logging
import pandas as pd
from datetime import datetime


def iter_filter(string):
    s = string.replace(" ", "")
    if s == "":
        return
    for pair in s.split("&"):
        key, value = pair.split("=")
        yield key, value


def filter2query(string):
    dct = dict(iter_filter(string))
    for key, value in list(dct.items()):
        if "," in value:
            for v in value.split(","):
                dct.setdefault("$or", []).append({key: v})
    return dct


def fields2projection(string):
    if string != "":
        return {key: 1 for key in string.replace(" ", "").split(",")}
    else:
        return {}


class CollectionFilter(object):

    def __call__(self, _filter):
        dct = dict(iter_filter(_filter))
        self._adapt(dct)
        return dct

    def _adapt(self, dct):
        for key, value in list(dct.items()):
            if "," in value:
                del dct[key]
                for v in value.split(","):
                    dct.setdefault("$or", []).append({key: v})


class InstrumentInfoFilter(CollectionFilter):

    def _adapt(self, dct):
        if dct['symbol'] == "":
            del dct["symbol"]
        super(InstrumentInfoFilter, self)._adapt(dct)


class RangeFilter(CollectionFilter):

    def __init__(self, **trans):
        self.trans = trans

    def _adapt(self, dct):
        super(RangeFilter, self)._adapt(dct)
        for key, value in self.trans.items():
            start = dct.pop("start_%s" % value, None)
            if start:
                dct[key] = {"$gte": start}
            end = dct.pop("end_%s" % value, None)
            if end:
                dct.setdefault(key, {})["$lte"] = end


class TradeCalFilter(CollectionFilter):

    def _adapt(self, dct):
        super(TradeCalFilter, self)._adapt(dct)
        start = dct.pop('start_date', None)
        if start:
            dct["trade_date"] = {"$gte": int(start)}
        end = dct.pop("end_date", None)
        if end:
            dct.setdefault("trade_date", {})["$lte"] = int(end)


class DBFilter(object):

    def __init__(self, key="symbol", **trans):
        self.key = key
        self.trans = trans

    def __call__(self, _filter):
        return self.__iter__(_filter)

    def __iter__(self, _filter):
        dct = dict(iter_filter(_filter))
        values = dct.pop(self.key, None)
        if not values or (values == ""):
            raise ValueError("%s is required" % self.key)
        self._adapt(dct)
        self.range(dct)
        for value in values.split(","):
            yield value, dct.copy()

    def _adapt(self, dct):
        for key, value in list(dct.items()):
            if "," in value:
                del dct[key]
                for v in value.split(","):
                    dct.setdefault("$or", []).append({key: v})

    def range(self, dct):
        for key, value in self.trans.items():
            start = dct.pop("start_%s" % value, None)
            if start:
                dct[key] = {"$gte": start}
            end = dct.pop("end_%s" % value, None)
            if end:
                dct.setdefault(key, {})["$lte"] = end


VIEWS = [
    (InstrumentInfoTable, InstrumentInfoFilter()),
    (SecTradeCalTable, TradeCalFilter()),
    (SecDividendTable, RangeFilter(exdiv_date="date")),
    (SecSuspTable, RangeFilter(ann_date='date')),
    (SecIndustryTable, CollectionFilter()),
    (ProfitExpressTable, RangeFilter(ann_date="anndate", report_date="reportdate")),
    (SecRestrictedTable, RangeFilter(list_date="date")),
    (IndexConsTable, RangeFilter(in_date="date")),
    (SecAdjFactorTable, DBFilter(trade_date="date")),
    (SecDailyIndicatorTable, DBFilter(trade_date="date")),
    (BalanceSheetTable, DBFilter(ann_date="date", act_ann_date="actdate", report_date="reportdate")),
    (IncomeTable, DBFilter(ann_date="date", act_ann_date="actdate", report_date="reportdate")),
    (CashFlowTable, DBFilter(ann_date="date", act_ann_date="actdate", report_date="reportdate")),
    (MFNavTable, DBFilter(ann_date="date", price_date="pdate")),
    (MFPortfolioTable, DBFilter(ann_date="date")),
    (MFDividendTable, DBFilter(ann_date="date")),
    (MFBondPortfolioTable, DBFilter(prt_enddate="date"))
]


def time_range(start=None, end=None):
    dct = {}
    if start:
        dct['$gte'] = datetime.strptime(start, "%Y-%m-%d").replace(hour=15)
    if end:
        dct["$lte"] = datetime.strptime(end, "%Y-%m-%d").replace(hour=15)
    return dct


class Factor(object):

    def __init__(self, client):
        self.client = client
        self.db = self.client["factor"]

    def read(self, _filter, fields):
        dct = dict(iter_filter(_filter))
        symbol = dct.pop("symbol")
        r = time_range(dct.pop('start', None), dct.pop("end", None))

        f = {'datetime': r} if len(r) else None
        projection = fields2projection(fields)
        if len(projection):
            projection['datetime'] = 1
        projection["_id"] = 0

        data = list(self.iter_read(symbol, f, projection))

        return pd.concat(data)

    def iter_read(self, symbols, _filter, projection):
        for symbol in symbols.split(","):
            try:
                yield self._read(symbol, _filter, projection.copy())
            except Exception as e:
                logging.error("factor(symbol=%s, filter=%s, projection=%s) error: %s",
                              symbol, _filter, projection, e)
                return pd.DataFrame()

    def _read(self, symbol, _filter, projection):
        data = pd.DataFrame(list(self.db[symbol].find(_filter, projection)))
        data["symbol"] = symbol
        data["datetime"] = data["datetime"].map(self.dt2int)
        return data

    @staticmethod
    def dt2int(time):
        return time.year*10000+time.month*100+time.day


class JsetReplier(MongodbHandler):

    def __init__(self, client):
        super(JsetReplier, self).__init__(client)
        self.views = {table.qs.view: (table, ft) for table, ft in VIEWS}
        self.factor = Factor(client)

    def receive(self, view, filter, fields, **kwargs):
        if view == "factor":
            data = self.factor.read(filter, fields)
        else:
            try:
                item = self.views[view]
                table, analysis = item
            except KeyError:
                error_msg = "View: %s not supported" % view
                raise KeyError(error_msg)

            if isinstance(table, Table2D):
                db, col = table.db.split(".")
                data = self.query_2d(self.client[db][col], analysis(filter), fields)
            elif isinstance(table, Table3D):
                data = pd.concat(list(self.iter_3d(self.client[table.db], analysis(filter), fields)))
            else:
                data = pd.DataFrame()

        return {key: item.tolist() for key, item in data.items()}

    def query_2d(self, collection, _filter, fields):
        data = read(collection,
                    _filter,
                    fields2projection(fields))
        return data

    def iter_3d(self, db, filters, fields):
        for col, f in filters:
            yield self.query_2d(db[col], f, fields)


if __name__ == '__main__':
    from pymongo import MongoClient

    client = MongoClient(port=37017)
    factor = Factor(client)
    print(factor.read("symbol=000001.XSHE&start=2017-01-01", "PB,PE,datetime"))