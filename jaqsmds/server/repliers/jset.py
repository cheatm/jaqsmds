from jaqsmds.server.repliers.utils import iter_filter, split_conditions, field_filter, MongodbHandler, \
    Jset3DReader, Jset2DReader
from jaqsmds.server.repliers.factor import FactorReader
from functools import partial


InstrumentInfoReader = partial(Jset2DReader)
SecTradeCalReader = partial(Jset2DReader, ranges={"date": "trade_date"})
SecDividendReader = partial(Jset2DReader, ranges={"date": "ann_date"})
SecAdjFactorReader = partial(Jset2DReader, ranges={"date": "date"})
SecSuspReader = partial(Jset2DReader, ranges={"date": "ann_date"})
SecIndustryReader = partial(Jset2DReader)
# SecDailyIndicatorReader = partial(JsetReader2D, ranges={"date": "trade_date"})
BalanceSheetReader = partial(
    Jset2DReader,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
IncomeReader = partial(
    Jset2DReader,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
CashFlowReader = partial(
    Jset2DReader,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
ProfitExpressReader = partial(
    Jset2DReader,
    ranges={"anndate": "ann_date", "reportdate": "report_date"}
)
SecRestrictedReader = partial(Jset2DReader, ranges={"date": "list_date"})
IndexConsReader = partial(Jset2DReader, ranges={"date": "in_date"})


LB = {"lb.secDividend": SecDividendReader,
      "lb.secSusp": SecSuspReader,
      "lb.secIndustry": SecIndustryReader,
      "lb.secAdjFactor": SecAdjFactorReader,
      # "lb.secDailyIndicator": SecDailyIndicatorReader,
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


class JsetReader(MongodbHandler):

    def __init__(self, client, lb=None, jz=None, factor=None):
        super(JsetReader, self).__init__(client)
        self.handlers = {}

        if lb:
            self.handlers.update(lb_readers(self.client[lb]))
        if jz:
            self.handlers.update(jz_readers(self.client[jz]))
        if factor:
            self.handlers['factor'] = FactorReader(self.client[factor])

    def receive(self, view, filter, fields, **kwargs):
        reader = self.handlers.get(view, None)
        if reader is not None:
            return reader.read(filter, fields)
        else:
            raise ValueError("Invalid view: %s" % view)