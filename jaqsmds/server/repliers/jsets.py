from jaqsmds.server.repliers.utils import QueryInterpreter as Qi, MongodbHandler, ColReader, DBReader
from jaqsmds.server.repliers.factor import FactorReader, DailyFactorReader
from functools import partial

SymbolQI = partial(Qi, primary="symbol")


InstrumentInfo = Qi("jz.instrumentInfo", defaults=['list_date', 'name', 'symbol'])
SecDividend = Qi(
    "lb.secDividend",
    defaults=['ann_date', 'bonus_list_date', 'cash', 'cash_tax', 'cashpay_date', 'div_enddate',
              'exdiv_date', 'publish_date', 'record_date', 'share_ratio', 'share_trans_ratio', 'symbol'],
    **{"date": "ann_date"}
)
SecAdjFactor = Qi("lb.secAdjFactor", defaults=['adjust_factor', 'symbol', 'trade_date'], **{"date": "trade_date"})
SecSusp = Qi("lb.secSusp",
             defaults=['ann_date', 'resu_date', 'susp_date', 'susp_reason', 'symbol'], **{"date": "ann_date"})
# SecIndustry = Qi("lb.secIndustry",
#                  defaults=['in_date', 'industry1_code', 'industry1_name', 'industry2_code',
#                            'industry2_name', 'industry3_code', 'industry3_name', 'industry4_code',
#                            'industry4_name', 'industry_src', 'out_date', 'symbol'])
SecDailyIndicator = Qi("lb.secDailyIndicator",
                       defaults=['symbol', 'trade_date'],
                       primary="symbol",
                       **{"date": "trade_date"})
BalanceSheet = SymbolQI(
    "lb.balanceSheet",
    defaults=['acct_rcv', 'ann_date', 'inventories', 'notes_rcv',
              'report_date', 'report_type', 'symbol', 'tot_cur_assets'],
    **{"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
Income = SymbolQI(
    "lb.income",
    defaults=['ann_date', 'int_income', 'tot_oper_cost', 'net_int_income', 'oper_exp', 'oper_profit',
              'oper_rev', 'report_date', 'symbol', 'less_handling_chrg_comm_exp', 'tot_profit', 'total_oper_rev'],
    **{"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
CashFlow = SymbolQI(
    "lb.cashFlow",
    defaults=['ann_date', 'cash_recp_prem_orig_inco', 'cash_recp_return_invest', 'cash_recp_sg_and_rs',
              'incl_dvd_profit_paid_sc_ms', 'net_cash_flows_inv_act', 'net_cash_received_reinsu_bus',
              'net_incr_dep_cob', 'net_incr_disp_tfa', 'net_incr_fund_borr_ofi', 'net_incr_insured_dep',
              'net_incr_int_handling_chrg', 'net_incr_loans_central_bank', 'other_cash_recp_ral_fnc_act',
              'other_cash_recp_ral_oper_act', 'recp_tax_rends', 'report_date', 'report_type',
              'stot_cash_inflows_oper_act', 'stot_cash_outflows_oper_act', 'symbol'],
    **{"date": "ann_date", "actdate": "act_ann_date", "reportdate": "report_date"}
)
ProfitExpress = SymbolQI(
    "lb.profitExpress",
    defaults=['ann_date', 'net_profit_int_inc', 'oper_profit', 'oper_rev',
              'report_date', 'symbol', 'total_assets', 'total_profit'],
    **{"anndate": "ann_date", "reportdate": "report_date"}
)
SecRestricted = SymbolQI("lb.secRestricted", defaults=['lifted_shares', 'list_date', 'symbol'], **{"date": "list_date"})
IndexWeightRange = Qi("lb.indexWeightRange",
                      defaults=['index_code', 'symbol', 'trade_date', 'weight'],
                      primary="index_code",
                      **{"date": "trade_date"})
FinIndicator = SymbolQI("lb.finIndicator",
                        defaults=['ann_date', 'bps', 'report_date', 'roa', 'roe', 'symbol'],
                        **{"date": "ann_date"})


class SecTradeCalInterpreter(Qi):

    def catch(self, dct):
        start = dct.pop("start_date", 0)
        end = dct.pop("end_date", 99999999)
        yield "trade_date", {"$gte": int(start), "$lte": int(end)}


SecTradeCal = SecTradeCalInterpreter("jz.secTradeCal", defaults=['istradeday', 'trade_date'])


class IndexConsInterpreter(Qi):

    def catch(self, dct):
        start = dct.pop("start_date")
        end = dct.pop('end_date')
        yield "in_date", (None, end)
        yield "out_date", [{"$gt": start}, "", None]


IndexCons = IndexConsInterpreter("lb.indexCons", primary='index_code',
                                 defaults=['in_date', 'index_code', 'out_date', 'symbol'])


class SecIndustryInterpreter(Qi):

    def catch(self, dct):
        i_s = dct.pop("industry_src", None)
        if i_s:
            yield "industry_src", i_s.lower()

        yield from super(SecIndustryInterpreter, self).catch(dct)

SecIndustry = SecIndustryInterpreter(
    "lb.secIndustry",
    defaults=['in_date', 'industry1_code', 'industry1_name', 'industry2_code',
              'industry2_name', 'industry3_code', 'industry3_name', 'industry4_code',
              'industry4_name', 'industry_src', 'out_date', 'symbol']
)


LB = [SecDividend, SecSusp, SecIndustry, SecAdjFactor, BalanceSheet, Income, CashFlow,
      ProfitExpress, SecRestricted, IndexCons, IndexWeightRange, FinIndicator]

JZ = [InstrumentInfo, SecTradeCal]

DBS = {SecDailyIndicator.view: SecDailyIndicator}


def col_readers(db, interpreters):
    dct = {}
    for interpreter in interpreters:
        collection = db[interpreter.view[3:]]
        dct[interpreter.view] = ColReader(collection, interpreter)

    return dct


class JsetHandler(MongodbHandler):

    def __init__(self, client, lb=None, jz=None, factor=None, **other):
        super(JsetHandler, self).__init__(client)
        self.handlers = {}

        if lb:
            self.handlers.update(col_readers(self.client[lb], LB))

        if jz:
            self.handlers.update(col_readers(self.client[jz], JZ))

        if factor:
            # self.handlers["factor"] = FactorReader(self.client[factor])
            self.handlers["factor"] = DailyFactorReader(self.client[factor])

        for view, db in other.items():
            interpreter = DBS.get(view, None)
            if interpreter:
                self.handlers[view] = DBReader(self.client[db], interpreter)

    def __getitem__(self, item):
        return self.handlers.get(item, None)

    def receive(self, view, filter, fields, **kwargs):
        parser = self.handlers.get(view, None)
        if parser is not None:
            return parser.parse(filter, fields)
        else:
            raise ValueError("Invalid view: %s" % view)