from datetime import datetime
from collections import Iterable
import pandas as pd
import logging


def expand(code):
    if code.endswith(".SH"):
        return code[:-3] + ".XSHG"
    else:
        return code[:-3] + ".XSHE"


def no_error(dct):
    dct["error"] = {"error": 0}


def date2str(t):
    return t.strftime("%Y%m%d")


def field_filter(string):
    dct = {s: 1 for s in string.replace(" ", "").split(",")}
    dct.pop("", None)
    dct["_id"] = 0
    return dct


def fill_field_filter(string, *args):
    dct = field_filter(string)
    if len(dct) > 1:
        for name in args:
            dct[name] = 1
    return dct


def iter_filter(string):
    s = string.replace(" ", "")
    if s == "":
        return
    for pair in s.split("&"):
        key, value = pair.split("=")
        if len(value):
            yield key, value


def _split_conditions(dct):
    for key, value in list(dct.items()):
        if ',' in value:
            del dct[key]
            yield "$or", [{key: v} for v in value.split(",")]
        else:
            yield key, value


def split_conditions(dct):
    for key, value in _split_conditions(dct):
        if isinstance(value, list):
            yield {key: value}


def time_range_daily(start=None, end=None):
    dct = {}
    if start:
        dct['$gte'] = datetime.strptime(start.replace("-", ""), "%Y%m%d").replace(hour=15)
    if end:
        dct["%lte"] = datetime.strptime(start.replace("-", ""), "%Y%m%d").replace(hour=15)
    return dct


class MongodbHandler(object):

    def __init__(self, client):
        self.client = client

    def handle(self, dct):
        dct = dct.copy()
        logging.warning("Message | %s", dct)
        try:
            result = self.receive(**dct.pop("params"))
            result = {name: item.tolist() for name, item in result.items()}
        except Exception as e:
            dct["error"] = {"error": -1, "message": str(e)}
            dct["result"] = {}
            logging.error('handler | %s', e)
        else:
            dct["result"] = result
            no_error(dct)

        dct["time"] = datetime.now().timestamp() * 1000
        logging.warning("Reply | id=%s", dct.get('id', None))
        return dct

    def receive(self, **kwargs):
        pass


class DBHandler(MongodbHandler):

    def __init__(self, client, db, tag=None):
        super(DBHandler, self).__init__(client)
        self.db = self.client[db]
        self.tag = "{} | %s | %s".format(tag or db)

    def receive(self, **kwargs):
        names, args, kws = self.adapt(**kwargs)
        return pd.concat(list(self.iter_read(names, *args, **kws)), ignore_index=True)

    def adapt(self, **kwargs):
        raise NotImplementedError("func: adapt should be implemented.")

    def iter_read(self, iters, *args, **kwargs):
        for name in iters:
            try:
                yield self.read_one(name, *args, **kwargs)
            except Exception as e:
                logging.error(self.tag, name, e)

    def read_one(self, name, *args, **kwargs):
        raise NotImplementedError("func: _read should be implemented.")


class JsetReaderInterface(object):

    def __init__(self, ranges=None, view=None, defaults=None, hint=None):
        self.view = view
        self.ranges = ranges if isinstance(ranges, dict) else {}
        if isinstance(defaults, dict):
            self.defaults = defaults
        elif isinstance(defaults, Iterable):
            self.defaults = dict.fromkeys(defaults, 1)
        else:
            self.defaults = {}
        self.hint = hint

    def read(self, filter, view):
        raise NotImplementedError("Should implement method: read")

    def split_range(self, dct):
        for key, value in self.ranges.items():
            r = {}
            start = dct.pop("start_%s" % key, None)
            if start:
                r["$gte"] = start
            end = dct.pop("end_%s" % key, None)
            if end:
                r["$lte"] = end
            if len(r):
                yield value, r

    def catch(self, dct):
        yield from self.split_range(dct)
        yield from _split_conditions(dct)

    def split_filter(self, dct):
        f = {}
        ors = []
        for key, value in self.catch(dct):
            if key != "$or":
                f[key] = value
            else:
                ors.append({key: value})

        if len(ors) == 1:
            f.update(ors[0])
        elif len(ors) > 1:
            f["$and"] = ors
        return f


class Jset2DReader(JsetReaderInterface):

    def __init__(self, collection, *args, **kwargs):
        super(Jset2DReader, self).__init__(*args, **kwargs)
        self.collection = collection

    def read(self, filter, fields):
        f = self.split_filter(dict(iter_filter(filter)))
        p = field_filter(fields)
        p.update(self.defaults)
        return pd.DataFrame(list(self.cursor(f, p)))

    def cursor(self, f, p):
        cursor = self.collection.find(f, p)
        if isinstance(self.hint, str):
            return cursor.hint([(self.hint, 1)])
        else:
            return self.cursor(f, p)


class Jset3DReader(JsetReaderInterface):

    field_filter = staticmethod(field_filter)

    def __init__(self, db, key="symbol", *args, **kwargs):
        super(Jset3DReader, self).__init__(*args, **kwargs)
        self.db = db
        self.key = key

    def read(self, filter, fields):
        symbols, f, p = self.adapt(filter, fields)
        return pd.concat(list(self.iter_read(symbols, f, p)))

    def iter_read(self, symbols, f, p):
        for symbol in symbols:
            try:
                yield self._read(symbol, f, p)
            except Exception as e:
                logging.error("%s | %s | %s", self.view, symbol, e)

    def _read(self, symbol, f, p):
        return pd.DataFrame(list(self.db[symbol].find(f, p)))

    def adapt(self, filter, fields):
        dct = dict(iter_filter(filter))
        symbols = self.catch_key(dct)
        f = self.split_filter(dct)
        p = self.field_filter(fields)
        p.update(self.defaults)
        return symbols, f, p

    def catch_key(self, dct):
        return dct.pop(self.key).split(",")


class QueryInterpreter(object):

    def __init__(self, view, defaults=None, primary=None, **ranges):
        self.view = view
        self.ranges = ranges
        self.primary = primary
        self.defaults = dict.fromkeys(defaults, 1) if defaults else {}

    def __call__(self, filter, fields):
        return dict(self.filter(filter)), self.fields(fields)

    def filter(self, string):
        single = {}
        for key, value in iter_filter(string):
            if "," in value:
                yield key, value.split(",")
            else:
                single[key] = value

        yield from self.catch(single)
        yield from single.items()

    def catch(self, dct):
        for key, value in self.ranges.items():
            start = dct.pop("start_%s" % key, None)
            end = dct.pop("end_%s" % key, None)
            if start or end:
                yield value, (start, end)

    def fields(self, string):
        fields = field_filter(string)
        fields.update(self.defaults)
        return fields


class ColReader(object):

    def __init__(self, collection, interpreter):
        self.collection = collection
        self.interpreter = interpreter

    @staticmethod
    def create_filter(dct):
        ands = []
        for key, value in dct.items():
            if isinstance(value, list):
                ands.append([{key: name} for name in value])
            elif isinstance(value, tuple):
                _r = {}
                start, end = value
                if start:
                    _r['$gte'] = start
                if end:
                    _r["$lte"] = end
                yield key, _r
            else:
                yield key, value

        if len(ands) == 1:
            yield "$or", ands[0]
        elif len(ands) > 1:
            yield "$and", [{"$or": item} for item in ands]

    def parse(self, filter, fields):
        query, projections = self.interpreter(filter, fields)
        filters = dict(self.create_filter(query))
        cursor = self.collection.find(filters, projections)
        if self.interpreter.primary:
            cursor.hint([(self.interpreter.primary, 1)])

        return pd.DataFrame(list(cursor))


class DBReader(object):

    def __init__(self, db, interpreter):
        self.db = db
        self.interpreter = interpreter
        self.view = interpreter.view
        self.primary = interpreter.primary

    def parse(self, filter, fields):
        query, projection = self.interpreter(filter, fields)
        primary = query.pop(self.primary)
        if not isinstance(primary, list):
            primary = [primary]
        filters = dict(self.create_filter(query))
        return pd.concat(list(self._parse(primary, filters, projection)))

    def _parse(self, cols, filters, projection):
        for name in cols:
            try:
                yield self.read(name, filters, projection)
            except Exception as e:
                logging.error("%s | %s | %s", self.view, name, e)

    def read(self, name, filters, projection):
        collection = self.db[name]
        cursor = collection.find(filters, projection)
        return pd.DataFrame(list(cursor))

    @staticmethod
    def create_filter(dct):
        ands = []
        for key, value in dct.items():
            if isinstance(value, list):
                ands.append([{key: name} for name in value])
            elif isinstance(value, tuple):
                _r = {}
                start, end = value
                if start:
                    _r['$gte'] = start
                if end:
                    _r["$lte"] = end
                yield key, _r
            else:
                yield key, value

        if len(ands) == 1:
            yield "$or", ands[0]
        elif len(ands) > 1:
            yield "$and", ands