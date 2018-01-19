from jaqsmds.server.repliers.utils import MongodbHandler, Jset3DReader, Jset2DReader
from jaqsmds.server.repliers.factor import FactorReader
from functools import partial


InstrumentInfoReader = partial(Jset2DReader)
SecTradeCalReader = partial(Jset2DReader, ranges={"date": "trade_date"})
SecDividendReader = partial(Jset2DReader, ranges={"date": "ann_date"})
SecAdjFactorReader = partial(Jset2DReader, ranges={"date": "date"})
SecSuspReader = partial(Jset2DReader, ranges={"date": "ann_date"})
SecIndustryReader = partial(Jset2DReader)
SecDailyIndicatorReader = partial(Jset3DReader, ranges={"date": "trade_date"})
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
# IndexConsReader = partial(Jset2DReader, ranges={"date": "in_date"})
MFNavReader = partial(Jset3DReader, ranges={"date": "ann_date", "pdate": "price_date"})
MFDividendReader = partial(Jset3DReader, ranges={"date": "ann_date"})
MFPortfolioReader = partial(Jset3DReader, ranges={"date": "ann_date"})
MFBondPortfolioReader = partial(Jset3DReader, ranges={"date": "ann_date"})


class IndexConsReader(Jset2DReader):

    def split_range(self, dct):
        start = dct.pop("start_date")
        end = dct.pop('end_date')
        yield "in_date", {"$lte": end}
        yield "$or", [{"out_date": {"$gt": start}}, {'out_date': ""}, {"out_date": None}]


LB = {"lb.secDividend": SecDividendReader,
      "lb.secSusp": SecSuspReader,
      "lb.secIndustry": SecIndustryReader,
      "lb.secAdjFactor": SecAdjFactorReader,
      "lb.balanceSheet": BalanceSheetReader,
      "lb.income": IncomeReader,
      "lb.cashFlow": CashFlowReader,
      "lb.profitExpress": ProfitExpressReader,
      "lb.secRestricted": SecRestrictedReader,
      "lb.indexCons": IndexConsReader}


def lb_readers(db):
    return {name: cls(db[name[3:]], view=name) for name, cls in LB.items()}


JZ = {"jz.instrumentInfo": InstrumentInfoReader,
      "jz.secTradeCal": SecTradeCalReader}


def jz_readers(db):
    return {name: cls(db[name[3:]], view=name) for name, cls in JZ.items()}


DB_READER = {
    "lb.secDailyIndicator": SecDailyIndicatorReader,
    "lb.mfNav": MFNavReader,
    "lb.mfDividend": MFDividendReader,
    "lb.mfPortfolio": MFPortfolioReader,
    "lb.mfBondPortfolio": MFBondPortfolioReader,
}


def iter_db_readers(client, dct):
    for view, cls in DB_READER.items():
        db = dct.get(view, None)
        if db:
            yield view, cls(client[db], view=view)


class JsetHandler(MongodbHandler):

    def __init__(self, client, lb=None, jz=None, factor=None, **other):
        super(JsetHandler, self).__init__(client)
        self.handlers = {}

        if lb:
            self.handlers.update(lb_readers(self.client[lb]))
        if jz:
            self.handlers.update(jz_readers(self.client[jz]))
        if factor:
            self.handlers['factor'] = FactorReader(self.client[factor])

        self.handlers.update(dict(iter_db_readers(self.client, other)))

    def receive(self, view, filter, fields, **kwargs):
        reader = self.handlers.get(view, None)
        if reader is not None:
            return reader.read(filter, fields)
        else:
            raise ValueError("Invalid view: %s" % view)
