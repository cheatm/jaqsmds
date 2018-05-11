from datautils.fxdayu import instance
from datautils.fxdayu import conf
from jaqsmds.server.repliers import jsets
from jaqsmds.server.repliers.utils import Handler
from jaqsmds.server.repliers.daxis import DailyFactor, DailyIndicator
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


class MultiViewReader(object):

    def __init__(self, interpreter, reader=None):
        self.interpreter = interpreter
        self.reader = reader if reader is not None else get_reader(interpreter.view)

    def __call__(self, filter, fields):
        result = self.read(filter, fields)
        dct = {}

        for name, value in result.items():
            dct[name] = self.handle(name, value)
        data = pd.Panel.from_dict(dct)
        return data.to_frame(True).swaplevel().sort_index().reset_index()

    def read(self, filter, fields):
        names, filters, prj = self.interpreter(filter, fields)
        return self.reader(names, index=self.interpreter.default, fields=prj, **filters)

    def handle(self, name, value):
        value.columns.name = self.interpreter.axis
        return value


class FactorReader(MultiViewReader):

    def __init__(self):
        super(FactorReader, self).__init__(
            DailyFactor,
            instance.api.factor,
        )

    def handle(self, name, value):
        return super(FactorReader, self).handle(
            name, value.rename_axis(time2int).rename_axis(expand, 1)
        )


class DailyIndicatorReader(MultiViewReader):

    def __init__(self):
        super(DailyIndicatorReader, self).__init__(
            DailyIndicator,
            instance.api.daily_indicator,
        )

    def handle(self, name, value):
        return super(DailyIndicatorReader, self).handle(
            name, value.rename_axis(expand, 1)
        )


class JsetHandler(Handler):

    def __init__(self):
        self.methods = {}
        for interpreter in jsets.JZ:
            self.methods[interpreter.view] = ViewReader(interpreter)
        for interpreter in jsets.LB:
            self.methods[interpreter.view] = ViewReader(interpreter)
        self.methods[DailyIndicator.view] = DailyIndicatorReader()
        self.methods[DailyFactor.view] = FactorReader()

    def receive(self, view, filter, fields, **kwargs):
        try:
            method = self.methods[view]
        except KeyError:
            raise KeyError("No such view: %s" % view)

        return method(filter, fields)
