from jaqsmds.server.repliers import jsets
from jaqsmds.server.repliers.utils import Handler, int2date, date2int, RangeInterpreter
from datautils.fxdayu import instance
import pandas as pd
import numpy as np


DEFAUT_IPT = RangeInterpreter("external", defaults={"symbol", "trade_date"}, date="trade_date")


def get_reader(name):
    # return instance.api.__getattribute__(VIEW_KEY_MAP[name].lower())
    return instance.api[name]


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
                "fxdayu.factor": "fxdayu_factor",
                'lb.secDailyIndicator': 'DAILY_INDICATOR',
                'lb.secAdjFactor': "SEC_ADJ_FACTOR",
                'updateStatus': "UPDATE_STATUS"}


class ViewReader(object):

    def __init__(self, interpreter, reader=None):
        self.interpreter = interpreter
        self.reader = reader if reader is not None else get_reader(interpreter.view)

    def __call__(self, filter, fields):
        filters, prj = self.interpreter(filter, fields)
        result = self.reader(fields=prj, **filters)
        filled = result.select_dtypes(exclude=[np.number]).fillna("")
        result[filled.columns] = filled
        if self.interpreter.sort:
            return result.sort_values(self.interpreter.sort)
        else:
            return result

    def read(self, filter, fields):
        filters, prj = self.interpreter(filter, fields)
        result = self.reader(fields=prj, **filters)
        return result


def expand(symbol=""):
    if symbol.startswith("6"):
        return symbol + ".SH"
    else:
        return symbol + ".SZ"


def time2int(time):
    return time.year*10000+time.month*100+time.day


class JsetHandler(Handler):

    def __init__(self):
        self.methods = {}
        for name, interpreter in jsets.API_JSET_MAP.items():
            self.methods[interpreter.view] = ViewReader(interpreter, instance.api.__getattribute__(name))
        self.methods["help.predefine"] = predefine

    def receive(self, view, filter, fields, **kwargs):
        try:
            method = self.methods[view]
        except KeyError:
            if view in instance.api.methods:
                method = ViewReader(DEFAUT_IPT, instance.api[view])
            else:
                raise KeyError("No such view: %s" % view)
        return method(filter, fields)


def predefine(*args, **kwargs):
    return instance.api.predefine()


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

    default_fields = {"trade_date", "symbol", "open", "high", "low", "close", "volume", "turnover", "vwap"}
    composory_fields = {"trade_date", "symbol"}

    def __init__(self):
        self.trade_cal = instance.api.trade_cal(fields="trade_date").set_index("trade_date").index.map(int).sort_values()

    def receive(self, symbol, begin_date, end_date, fields="", adjust_mode="none", freq="1d", **kwargs):
        # Modify inputs
        symbol = symbol.split(",")
        if len(fields):
            fields = set(fields.split(","))
            fields.update(self.composory_fields)
        else:
            fields = self.default_fields

        # Read original data
        data = instance.api.daily(symbol, begin_date, end_date, fields).sort_values(["symbol", "trade_date"]).set_index(["symbol", "trade_date"])
        if adjust_mode != "none":
            adjust_factor = instance.api.sec_adj_factor(
                None, {"symbol", "trade_date", "adjust_factor"}, 
                trade_date=(begin_date, end_date), symbol=symbol
            ).reindex_axis(["symbol", "trade_date", "adjust_factor"], 1).sort_values(["symbol", "trade_date"])
            
            adjust_factor["trade_date"] = adjust_factor["trade_date"].apply(int)
            adjust_factor = adjust_factor.set_index(["symbol", "trade_date"])["adjust_factor"].reindex_axis(data.index).fillna(1)
            if adjust_mode == "post":
                self._adjust(data, adjust_factor)
            else:
                self._adjust(data, 1/adjust_factor)
            
        # Catch trade_dates
        trade_dates = list(self._get_trade_cal(begin_date, end_date))
        index = pd.MultiIndex.from_product([symbol, trade_dates], names=("symbol", "trade_date"))
        result = data.reindex(index)
        for name in ["volume", "turnover", "trade_status"]:
            if name in result.columns:
                result[name].fillna(0, inplace=True)
        return result.reset_index()

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
        adj = adj.drop_duplicates(["trade_date", "symbol"]).pivot("trade_date", "symbol", "adjust_factor").rename_axis(int)
        return pd.DataFrame(adj, trade_dates, symbol).ffill().fillna(1)

    def _get_trade_cal(self, start, end):
        sliced = slice(*self.trade_cal.slice_locs(start, end, kind="loc"))
        return self.trade_cal[sliced]


class JsiHandler(Handler):

    default_fields = {"close"}

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
        fields.update(self.default_fields)
        trade_date = int2date(trade_date)
        data = instance.api.bar(symbol, trade_date, fields)
        data["freq"] = freq
        print(data)
        if vwap:
            data["vwap"] = data["turnover"] / data["volume"].replace(0, np.NaN)
            null = data["vwap"].isnull()
            data["vwap"][null] = data["close"][null]
        return data
