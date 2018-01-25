from jaqsmds.server.repliers.utils import MongodbHandler, Jset3DReader, Jset2DReader
from jaqsmds.server.repliers.factor import FactorReader
from functools import partial


InstrumentInfoReader = partial(Jset2DReader, defaults=['list_date', 'name', 'symbol'])
SecDividendReader = partial(
    Jset2DReader, ranges={"date": "ann_date"},
    defaults=['ann_date', 'bonus_list_date', 'cash', 'cash_tax', 'cashpay_date', 'div_enddate',
              'exdiv_date', 'publish_date', 'record_date', 'share_ratio', 'share_trans_ratio', 'symbol']
)
SecAdjFactorReader = partial(Jset2DReader, ranges={"date": "trade_date"},
                             defaults=['adjust_factor', 'symbol', 'trade_date'])
SecSuspReader = partial(Jset2DReader, ranges={"date": "ann_date"},
                        defaults=['ann_date', 'resu_date', 'susp_date', 'susp_reason', 'symbol'])
SecIndustryReader = partial(Jset2DReader,
                            defaults=['in_date', 'industry1_code', 'industry1_name', 'industry2_code',
                                      'industry2_name', 'industry3_code', 'industry3_name', 'industry4_code',
                                      'industry4_name', 'industry_src', 'out_date', 'symbol'])
SecDailyIndicatorReader = partial(Jset3DReader, ranges={"date": "trade_date"}, defaults=['symbol', 'trade_date'])
BalanceSheetReader = partial(
    Jset2DReader,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"},
    defaults=['acct_rcv', 'ann_date', 'inventories', 'notes_rcv',
              'report_date', 'report_type', 'symbol', 'tot_cur_assets']
)
IncomeReader = partial(
    Jset2DReader,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"},
    defaults=['ann_date', 'int_income', 'tot_oper_cost', 'net_int_income', 'oper_exp', 'oper_profit',
              'oper_rev', 'report_date', 'symbol', 'less_handling_chrg_comm_exp', 'tot_profit', 'total_oper_rev']
)
CashFlowReader = partial(
    Jset2DReader,
    ranges={"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"},
    defaults=['ann_date', 'cash_recp_prem_orig_inco', 'cash_recp_return_invest', 'cash_recp_sg_and_rs',
              'incl_dvd_profit_paid_sc_ms', 'net_cash_flows_inv_act', 'net_cash_received_reinsu_bus',
              'net_incr_dep_cob', 'net_incr_disp_tfa', 'net_incr_fund_borr_ofi', 'net_incr_insured_dep',
              'net_incr_int_handling_chrg', 'net_incr_loans_central_bank', 'other_cash_recp_ral_fnc_act',
              'other_cash_recp_ral_oper_act', 'recp_tax_rends', 'report_date', 'report_type',
              'stot_cash_inflows_oper_act', 'stot_cash_outflows_oper_act', 'symbol']
)
ProfitExpressReader = partial(
    Jset2DReader,
    ranges={"anndate": "ann_date", "reportdate": "report_date"},
    defaults=['ann_date', 'net_profit_int_inc', 'oper_profit', 'oper_rev',
              'report_date', 'symbol', 'total_assets', 'total_profit']
)
SecRestrictedReader = partial(Jset2DReader, ranges={"date": "list_date"},
                              defaults=['lifted_shares', 'list_date', 'symbol'])
MFNavReader = partial(Jset3DReader, ranges={"date": "ann_date", "pdate": "price_date"})
MFDividendReader = partial(Jset3DReader, ranges={"date": "ann_date"})
MFPortfolioReader = partial(Jset3DReader, ranges={"date": "ann_date"})
MFBondPortfolioReader = partial(Jset3DReader, ranges={"date": "ann_date"})
IndexWeightRangeReader = partial(Jset2DReader, ranges={"date": "trade_date"},
                                 defaults=['index_code', 'symbol', 'trade_date', 'weight'])
FinIndicatorReader = partial(Jset2DReader, ranges={"date": "ann_date"},
                             defaults=['ann_date', 'bps', 'report_date', 'roa', 'roe', 'symbol'])


class SecTradeCalReader(Jset2DReader):

    def __init__(self, *args, **kwargs):
        super(SecTradeCalReader, self).__init__(*args, defaults=['istradeday', 'trade_date'], **kwargs,)

    def split_range(self, dct):
        start = dct.pop("start_date", 0)
        end = dct.pop("end_date", 99999999)
        yield "trade_date", {"$gte": int(start), "$lte": int(end)}


class IndexConsReader(Jset2DReader):

    def __init__(self, *args, **kwargs):
        super(IndexConsReader, self).__init__(*args, defaults=['in_date', 'index_code', 'out_date', 'symbol'], **kwargs)

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
      "lb.indexCons": IndexConsReader,
      "lb.indexWeightRange": IndexWeightRangeReader,
      "lb.finIndicator": FinIndicatorReader}


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

    def __getitem__(self, item):
        return self.handlers.get(item, None)