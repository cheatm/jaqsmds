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
            yield "$and", ands

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


if __name__ == '__main__':
    from pymongo import MongoClient

    col = MongoClient("192.168.0.102")["lb"]["income"]
    params = {'view': 'lb.income', 'fields': 'report_date,ebit,symbol,ann_date', 'filter': 'symbol=000001.SZ,000002.SZ,000008.SZ,000009.SZ,000024.SZ,000027.SZ,000039.SZ,000046.SZ,000060.SZ,000061.SZ,000063.SZ,000069.SZ,000100.SZ,000156.SZ,000157.SZ,000166.SZ,000333.SZ,000338.SZ,000400.SZ,000402.SZ,000413.SZ,000415.SZ,000423.SZ,000425.SZ,000503.SZ,000538.SZ,000539.SZ,000540.SZ,000555.SZ,000559.SZ,000568.SZ,000581.SZ,000598.SZ,000623.SZ,000625.SZ,000627.SZ,000629.SZ,000630.SZ,000651.SZ,000671.SZ,000686.SZ,000709.SZ,000712.SZ,000718.SZ,000723.SZ,000725.SZ,000728.SZ,000729.SZ,000738.SZ,000750.SZ,000768.SZ,000776.SZ,000778.SZ,000783.SZ,000792.SZ,000793.SZ,000800.SZ,000825.SZ,000826.SZ,000831.SZ,000839.SZ,000858.SZ,000876.SZ,000878.SZ,000883.SZ,000895.SZ,000898.SZ,000917.SZ,000937.SZ,000938.SZ,000959.SZ,000960.SZ,000961.SZ,000963.SZ,000970.SZ,000977.SZ,000983.SZ,000999.SZ,001979.SZ,002001.SZ,002007.SZ,002008.SZ,002024.SZ,002027.SZ,002038.SZ,002044.SZ,002049.SZ,002051.SZ,002065.SZ,002074.SZ,002081.SZ,002085.SZ,002129.SZ,002131.SZ,002142.SZ,002146.SZ,002152.SZ,002153.SZ,002174.SZ,002183.SZ,002195.SZ,002202.SZ,002230.SZ,002236.SZ,002241.SZ,002252.SZ,002292.SZ,002294.SZ,002299.SZ,002304.SZ,002310.SZ,002344.SZ,002352.SZ,002353.SZ,002375.SZ,002385.SZ,002399.SZ,002410.SZ,002411.SZ,002415.SZ,002422.SZ,002424.SZ,002426.SZ,002450.SZ,002456.SZ,002460.SZ,002465.SZ,002466.SZ,002468.SZ,002470.SZ,002475.SZ,002500.SZ,002508.SZ,002555.SZ,002558.SZ,002568.SZ,002570.SZ,002572.SZ,002594.SZ,002601.SZ,002602.SZ,002608.SZ,002624.SZ,002653.SZ,002673.SZ,002714.SZ,002736.SZ,002739.SZ,002797.SZ,002831.SZ,002839.SZ,002841.SZ,300002.SZ,300003.SZ,300015.SZ,300017.SZ,300024.SZ,300027.SZ,300033.SZ,300058.SZ,300059.SZ,300070.SZ,300072.SZ,300085.SZ,300104.SZ,300122.SZ,300124.SZ,300133.SZ,300136.SZ,300144.SZ,300146.SZ,300168.SZ,300182.SZ,300251.SZ,300315.SZ,600000.SH,600005.SH,600008.SH,600009.SH,600010.SH,600011.SH,600015.SH,600016.SH,600018.SH,600019.SH,600021.SH,600022.SH,600023.SH,600027.SH,600028.SH,600029.SH,600030.SH,600031.SH,600036.SH,600037.SH,600038.SH,600048.SH,600050.SH,600060.SH,600061.SH,600066.SH,600068.SH,600074.SH,600085.SH,600089.SH,600098.SH,600100.SH,600104.SH,600108.SH,600109.SH,600111.SH,600115.SH,600118.SH,600150.SH,600153.SH,600157.SH,600166.SH,600170.SH,600177.SH,600188.SH,600196.SH,600208.SH,600219.SH,600221.SH,600233.SH,600252.SH,600256.SH,600271.SH,600276.SH,600277.SH,600297.SH,600309.SH,600315.SH,600316.SH,600317.SH,600332.SH,600340.SH,600348.SH,600350.SH,600352.SH,600362.SH,600369.SH,600372.SH,600373.SH,600376.SH,600383.SH,600390.SH,600398.SH,600406.SH,600415.SH,600436.SH,600446.SH,600482.SH,600485.SH,600489.SH,600497.SH,600498.SH,600516.SH,600518.SH,600519.SH,600522.SH,600535.SH,600547.SH,600549.SH,600570.SH,600578.SH,600582.SH,600583.SH,600585.SH,600588.SH,600597.SH,600600.SH,600606.SH,600633.SH,600637.SH,600642.SH,600648.SH,600649.SH,600654.SH,600660.SH,600663.SH,600666.SH,600674.SH,600682.SH,600685.SH,600688.SH,600690.SH,600703.SH,600704.SH,600705.SH,600717.SH,600718.SH,600737.SH,600739.SH,600741.SH,600754.SH,600783.SH,600795.SH,600804.SH,600809.SH,600816.SH,600820.SH,600827.SH,600837.SH,600839.SH,600863.SH,600867.SH,600871.SH,600873.SH,600875.SH,600886.SH,600887.SH,600893.SH,600895.SH,600900.SH,600909.SH,600919.SH,600926.SH,600958.SH,600959.SH,600977.SH,600998.SH,600999.SH,601006.SH,601009.SH,601012.SH,601016.SH,601018.SH,601021.SH,601088.SH,601098.SH,601099.SH,601106.SH,601111.SH,601117.SH,601118.SH,601127.SH,601155.SH,601158.SH,601163.SH,601166.SH,601168.SH,601169.SH,601179.SH,601186.SH,601198.SH,601211.SH,601212.SH,601216.SH,601225.SH,601228.SH,601229.SH,601231.SH,601238.SH,601258.SH,601288.SH,601318.SH,601328.SH,601333.SH,601336.SH,601375.SH,601377.SH,601390.SH,601398.SH,601555.SH,601600.SH,601601.SH,601607.SH,601608.SH,601611.SH,601618.SH,601628.SH,601633.SH,601668.SH,601669.SH,601688.SH,601699.SH,601718.SH,601727.SH,601766.SH,601788.SH,601800.SH,601808.SH,601818.SH,601857.SH,601866.SH,601872.SH,601877.SH,601878.SH,601881.SH,601888.SH,601898.SH,601899.SH,601901.SH,601919.SH,601928.SH,601929.SH,601933.SH,601939.SH,601958.SH,601966.SH,601969.SH,601985.SH,601988.SH,601989.SH,601991.SH,601992.SH,601997.SH,601998.SH,603000.SH,603160.SH,603288.SH,603799.SH,603833.SH,603858.SH,603885.SH,603993.SH&start_date=20140623&end_date=20180123&report_type=408001000', 'order_by': 'report_date'}
    reader = ColReader(
        col,
        QueryInterpreter(primary="symbol", **{"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}),
    )

    print(reader.parse(
        params["filter"],
        params['fields']
    ))