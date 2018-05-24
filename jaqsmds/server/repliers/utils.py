from datetime import datetime
import pandas as pd
import traceback
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


def date2int(date):
    return date.year*10000+date.month*100+date.day


def int2date(num):
    day = _range(num % 100, 1, 31)
    month = _range(int(num/100) % 100, 1, 31)
    year = _range(int(num/10000), 1, 2029)
    return datetime(year, month, day)


def _range(value, _min, _max):
    if value > _max:
        return _max
    elif value < _min:
        return _min
    else:
        return value


def field_filter(string):
    return set(string.replace(" ", "").split(","))


def iter_filter(string):
    s = string.replace(" ", "")
    if s == "":
        return
    for pair in s.split("&"):
        key, value = pair.split("=")
        if len(value):
            yield key, value


class Handler(object):

    def handle(self, dct):
        dct = dct.copy()
        logging.warning("Message | %s", dct)
        try:
            result = self.receive(**dct.pop("params"))
            if isinstance(result, pd.DataFrame):
                result = result.to_dict("list")
        except Exception as e:
            dct["error"] = {"error": -1, "message": str(e)}
            logging.error('handler | %s', traceback.format_exc(5))
        else:
            dct["result"] = result
            no_error(dct)

        dct["time"] = datetime.now().timestamp() * 1000
        logging.warning("Reply | id=%s", dct.get('id', None))
        return dct

    def receive(self, **kwargs):
        pass


class QueryInterpreter(object):

    def __init__(self, view, defaults=None, primary=None, trans=None, **ranges):
        self.view = view
        self.ranges = ranges
        self.primary = primary
        self.defaults = defaults if defaults else set()
        self.trans = trans if isinstance(trans, dict) else {}

    def __call__(self, filter, fields):
        return dict(self.filter(filter)), self.fields(fields)

    def filter(self, string):
        single = {}
        filters = dict(iter_filter(string))
        yield from self.catch_trans(filters, single)
        for key, value in filters.items():
            if "," in value:
                yield key, set(value.split(","))
            else:
                single[key] = value
        yield from self.catch(single)
        yield from single.items()

    def catch_trans(self, dct, single):
        for key, method in self.trans.items():
            value = dct.pop(key, None)
            if value is not None:
                if "," in value:
                    yield key, set(map(method, value.split(",")))
                else:
                    single[key] = method(value)

    def catch(self, dct):
        for key, value in self.ranges.items():
            start = dct.pop("start_%s" % key, None)
            end = dct.pop("end_%s" % key, None)
            if start or end:
                yield value, (start, end)

    def fields(self, string):
        if string == "":
            return None
        fields = field_filter(string)
        fields.update(self.defaults)
        return list(fields)
