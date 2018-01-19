from datetime import datetime
import pandas as pd
import logging


def expand(code):
    if code.startswith("6"):
        return code + ".XSHG"
    else:
        return code + ".XSHE"


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
        logging.warning("Message: %s", dct)
        try:
            result = self.receive(**dct.pop("params"))
        except Exception as e:
            dct["error"] = {"error": -1, "message": str(e)}
            dct["result"] = {}
            logging.error('jset: %s', e)
        else:
            dct["result"] = result
            no_error(dct)

        dct["time"] = datetime.now().timestamp() * 1000
        return dct

    def receive(self, **kwargs):
        pass


class DBReader(MongodbHandler):

    def __init__(self, client, db, tag=None):
        super(DBReader, self).__init__(client)
        self.db = self.client[db]
        self.tag = "{} | %s | %s".format(tag or db)

    def receive(self, **kwargs):
        names, args, kws = self.adapt(**kwargs)
        data = pd.concat(list(self.iter_read(names, *args, **kws)))
        return {name: item.tolist() for name, item in data.items()}

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

    def __init__(self, ranges=None, view=None):
        self.view = view
        self.ranges = ranges if isinstance(ranges, dict) else {}

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

    def __init__(self, collection, ranges=None, view=None):
        super(Jset2DReader, self).__init__(ranges, view)
        self.collection = collection

    def read(self, filter, fields):
        f = self.split_filter(dict(iter_filter(filter)))
        data = pd.DataFrame(list(self.collection.find(f, field_filter(fields))))
        return {name: item.tolist() for name, item in data.items()}


class Jset3DReader(JsetReaderInterface):

    field_filter = staticmethod(field_filter)

    def __init__(self, db, key="symbol", ranges=None, view=None):
        super(Jset3DReader, self).__init__(ranges, view)
        self.db = db
        self.key = key

    def read(self, filter, fields):
        symbols, f, p = self.adapt(filter, fields)
        data = pd.concat(list(self.iter_read(symbols, f, p)))
        return {name: item.tolist() for name, item in data.items()}

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
        return symbols, f, p

    def catch_key(self, dct):
        return dct.pop(self.key).split(",")
