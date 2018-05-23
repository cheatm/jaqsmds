from jaqsmds.server.repliers import jsets
from jaqsmds.server.repliers.utils import Handler, int2date, date2int
from datautils.fxdayu import instance
import pandas as pd


def get_reader(name):
    return instance.api.__getattribute__(VIEW_KEY_MAP[name].lower())


VIEW_KEY_MAP = {'help.apiList': 'API_LIST',
                'help.apiParam': 'API_PARAM',
                'jz.instrumentInfo': 'INST_INFO',
                'jz.secTradeCal': 'TRADE_CAL',
                'lb.balanceSheet': 'BALANCE_SHEET',
                'lb.cashFlow': 'CASH_FLOW',
                'lb.finIndicator': 'FIN_INDICATOR',
                'lb.income': 'INCOME',
                'lb.indexCons': 'INDEX_CONS',
                'lb.indexWeightRange': 'INDEX_WEIGHT_RANGE',
                'lb.profitExpress': 'PROFIT_EXPRESS',
                'lb.sState': 'S_STATE',
                'lb.secDividend': 'SEC_DIVIDEND',
                'lb.secIndustry': 'SEC_INDUSTRY',
                'lb.secRestricted': 'SEC_RESTRICTED',
                'lb.secSusp': 'SEC_SUSP',
                'lb.windFinance': 'WIND_FINANCE',
                'JSI': 'STOCK_1M',
                'JSD': 'STOCK_D',
                'factor': 'FACTOR',
                'lb.secDailyIndicator': 'DAILY_INDICATOR',
                'lb.secAdjFactor': "SEC_ADJ_FACTOR"}


JSET_VIEWS = ['help.apiList', 'help.apiParam', 'jz.instrumentInfo', 'jz.secTradeCal', 'lb.balanceSheet', 'lb.cashFlow',
              'lb.finIndicator', 'lb.income', 'lb.indexCons', 'lb.indexWeightRange', 'lb.profitExpress', 'lb.sState',
              'lb.secDividend', 'lb.secIndustry', 'lb.secRestricted', 'lb.secSusp', 'lb.windFinance']


class ViewReader(object):

    def __init__(self, interpreter, reader=None):
        self.interpreter = interpreter
        self.reader = reader if reader is not None else get_reader(interpreter.view)

    def __call__(self, filter, fields):
        filters, prj = self.interpreter(filter, fields)
        result = self.reader(fields=prj, **filters)
        return result

    def read(self, filter, fields):
        filters, prj = self.interpreter(filter, fields)
        result = self.reader(fields=fields, **filters)
        return result


def expand(symbol=""):
    if symbol.startswith("6"):
        return symbol + ".SH"
    else:
        return symbol + ".SZ"


def time2int(time):
    return time.year*10000+time.month*100+time.day


DailyIndicator = jsets.Qi("lb.secDailyIndicator", date="trade_date")


class DailyFactorInterpreter(jsets.Qi):

    def __init__(self):
        super(DailyFactorInterpreter, self).__init__(view="factor")

    def catch(self, dct):
        start = dct.pop("start", None)
        end = dct.pop("end", None)

        if start or end:
            yield "datetime", (start, end)


DailyFactor = DailyFactorInterpreter()


class JsetHandler(Handler):

    def __init__(self):
        self.methods = {}
        for interpreter in jsets.JZ:
            self.methods[interpreter.view] = ViewReader(interpreter)
        for interpreter in jsets.LB:
            self.methods[interpreter.view] = ViewReader(interpreter)
        self.methods[DailyIndicator.view] = ViewReader(DailyIndicator)
        self.methods[DailyFactor.view] = ViewReader(DailyFactor)

    def receive(self, view, filter, fields, **kwargs):
        try:
            method = self.methods[view]
        except KeyError:
            raise KeyError("No such view: %s" % view)

        return method(filter, fields)


def unfold(symbol):
    if symbol.endswith(".SH"):
        return symbol[:-3] + ".XSHG"
    elif symbol.endswith(".SZ"):
        return symbol[:-3] + ".XSHE"
    else:
        return symbol


def fold(symbol):
    if symbol.endswith(".XSHG"):
        return symbol[:6] + ".SH"
    elif symbol.endswith(".XSHE"):
        return symbol[:6] + ".SZ"
    else:
        return symbol


def fold_code(symbol):
    return symbol[:6]


def merge(dct, vwap):
    data = pd.Panel.from_dict(dct).transpose(2, 1, 0)
    data.minor_axis.name = "symbol"
    if vwap:
        data["vwap"] = data["turnover"] / data["volume"]
    return data


class JsdHandler(Handler):

    def __init__(self):
        self.trade_cal = instance.api.trade_cal("trade_date").index

    def receive(self, symbol, begin_date, end_date, fields="", adjust_mode="none", freq="1d", **kwargs):
        # Modify inputs
        symbol = list(map(unfold, symbol.split(",")))
        start = int2date(begin_date).replace(hour=15)
        end = int2date(end_date).replace(hour=15)
        fields = set(fields.split(",")) if len(fields) else {}

        if (len(fields) == 0):
            vwap = True
            fields = None
        elif "vwap" in fields:
            fields.add("volume")
            fields.add("turnover")
            fields.remove("vwap")
            vwap = True
        else:
            vwap = False

        # Read original data
        data = instance.api.stock_d(symbol, "datetime", fields, datetime=(start, end))

        # Catch trade_dates
        trade_dates = self._get_trade_cal(begin_date, end_date)

        # Decorate data in Panel format
        for name, item in data.items():
            item["trade_status"] = 1
        data = merge(data, vwap)
        data.rename_axis(date2int, 1, inplace=True)
        data["code"] = pd.DataFrame({name: fold_code(name) for name in data.minor_axis}, data.major_axis)
        data["freq"] = freq
        data = data.reindex(major_axis=trade_dates)
        data.fillna(0, inplace=True)
        data.rename_axis(fold, 2, inplace=True)

        # Adjust price
        if adjust_mode != "none":
            adjust = self._adjust_factor(list(data.minor_axis), trade_dates)
            if adjust_mode == "post":
                self._adjust(data, adjust)
            else:
                self._adjust(data, 1/adjust)

        # Return in DataFrame format
        return data.to_frame(False).sortlevel("symbol").reset_index()

    def _adjust(self, data, adjust):
        for name in ["open", "high", "low", "close", "vwap"]:
            if name in data:
                data[name] *= adjust

    def _adjust_factor(self, symbol, trade_dates):
        start = str(trade_dates[0])
        end = str(trade_dates[-1])
        adj = instance.api.sec_adj_factor(trade_date=(start, end), symbol=symbol).reindex_axis(
            ["trade_date", "symbol", "adjust_factor"], 1
        )

        return adj.drop_duplicates(["trade_date", "symbol"]).pivot(
            "trade_date", "symbol", "adjust_factor"
        ).rename_axis(int).reindex(trade_dates).ffill().fillna(1)

    def _get_trade_cal(self, start, end):
        sliced = slice(*self.trade_cal.slice_locs(start, end, kind="loc"))
        return self.trade_cal[sliced]


class JsiHandler(Handler):

    def receive(self, symbol, begin_time, end_time, trade_date=0, freq="1M", fields="", **kwargs):
        symbol = list(map(unfold, symbol.split(",")))
        if fields == "":
            fields = "open,high,low,close,volume,turnover,vwap"
        fields = set(fields.split(","))
        if "vwap" in fields:
            fields.add("volume")
            fields.add("turnover")
            fields.remove("vwap")
            vwap = True
        else:
            vwap = False
        trade_date = int2date(trade_date)

        panel = self.read(symbol, begin_time, end_time, trade_date, freq, fields, vwap).rename_axis(fold, 2)
        result = panel.rename_axis(fold).to_frame(False).sortlevel("symbol").reset_index(level="symbol")
        result["code"] = result["symbol"].apply(lambda s: s[:6])
        result["trade_date"] = result["date"]
        return result

    def read(self, symbol, begin_time, end_time, trade_date, freq, fields, vwap=False):
        data = instance.api.stock_1m(symbol, "datetime", fields, _d=trade_date)
        data = merge(data, vwap)
        dates = list(map(lambda t: t.year*10000+t.month*100+t.day, data.major_axis))
        dates = pd.DataFrame({name: dates for name in data.minor_axis}, data.major_axis)
        data["date"] = dates
        times = list(map(lambda t: t.hour*10000+t.minute*100+t.second, data.major_axis))
        data["time"] = pd.DataFrame({name: times for name in data.minor_axis}, data.major_axis)
        data["code"] = pd.DataFrame({name: fold_code(name) for name in data.minor_axis}, data.major_axis)
        data["freq"] = freq
        return data


if __name__ == '__main__':
    from datautils.fxdayu import conf
    from jaqsmds import logger
    from datetime import datetime

    logger.init()
    conf.MONGODB_URI = "192.168.0.102"
    instance.init()
    # handler = JsiHandler()
    # print(handler.receive("000001.SZ,000002.SZ", 0, 0, 20180420))

    # handler = JsdHandler()
    # print(handler.receive(**{'symbol': '000016.SH', 'fields': 'trade_date,symbol,close,vwap,volume,turnover', 'begin_date': 20170102, 'end_date': 20170327, 'adjust_mode': 'post', 'freq': '1d'}))
    handler = JsetHandler()
    print(handler.receive("factor", "symbol=000001.SZ,600000.SH&start_date=20160505&end_date=20170304", "PB,A020006A"))