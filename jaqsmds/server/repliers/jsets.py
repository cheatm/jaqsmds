from jaqsmds.server.repliers.utils import QueryInterpreter as Qi


class SymbolQI(Qi):

    def __init__(self, view, *args, **kwargs):
        super(SymbolQI, self).__init__(view, *args, primary="symbol", **kwargs)


InstrumentInfo = Qi("jz.instrumentInfo", trans={"inst_type": int, "list_date": int, "status": int})
SecDividend = Qi(
    "lb.secDividend",
    defaults=['ann_date', 'bonus_list_date', 'cash', 'cash_tax', 'cashpay_date', 'div_enddate',
              'exdiv_date', 'publish_date', 'record_date', 'share_ratio', 'share_trans_ratio', 'symbol'],
    **{"date": "ann_date"}
)
SecAdjFactor = SymbolQI("lb.secAdjFactor", defaults=['adjust_factor', 'symbol', 'trade_date'], **{"date": "trade_date"})
SecSusp = Qi("lb.secSusp",
             defaults=['ann_date', 'resu_date', 'susp_date', 'susp_reason', 'symbol'],
             **{"date": "date"})
IndexCons = Qi("lb.indexCons", primary='index_code', defaults=['in_date', 'index_code', 'out_date', 'symbol'],
               **{"date": "date"})
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
ApiList = Qi("help.apiList")
ApiParam = Qi("help.apiParam")
WindFinance = SymbolQI("lb.windFinance", **{"date": "index"})
SecTradeCal = Qi("jz.secTradeCal", defaults=['istradeday', 'trade_date'],
                 trans={"start_date": int, "end_date": int},
                 date="trade_date")
SState = SymbolQI("lb.sState", trans={"start_date": int, "end_date": int}, date="dffDate")


class SecSuspInterpreter(Qi):

    def catch(self, dct):
        start = dct.pop("start_date", None)
        if start:
            yield "$or", [{"resu_date": {"$gte": start}}, {"resu_date": ""}]
        end = dct.pop("end_date", None)
        if end:
            yield "susp_date", (None, end)

#
# SecSusp = SecSuspInterpreter(
#     "lb.secSusp", defaults=['ann_date', 'resu_date', 'susp_date', 'susp_reason', 'symbol'], **{"date": "date"}
# )


class IndexConsInterpreter(Qi):

    def catch(self, dct):
        start = dct.pop("start_date")
        end = dct.pop('end_date')
        yield "in_date", (None, end)
        yield "out_date", (start, None)
#
#
# IndexCons = IndexConsInterpreter("lb.indexCons", primary='index_code',
#                                  defaults=['in_date', 'index_code', 'out_date', 'symbol'])


class SecIndustryInterpreter(Qi):

    def catch(self, dct):
        i_s = dct.pop("industry_src", None)
        if i_s:
            yield "industry_src", i_s.lower()

        yield from super(SecIndustryInterpreter, self).catch(dct)


SecIndustry = Qi(
    "lb.secIndustry",
    defaults=['in_date', 'industry1_code', 'industry1_name', 'industry2_code',
              'industry2_name', 'industry3_code', 'industry3_name', 'industry4_code',
              'industry4_name', 'industry_src', 'out_date', 'symbol'],
    trans={"industry_src": lambda s: s.lower()}
)


DailyIndicator = Qi("lb.secDailyIndicator", date="trade_date")
DailyFactor = Qi("factor", date="datetime")
FxdayuFactor = Qi("fxdayu.factor", date="datetime")
ViewFields = Qi("jz.viewFields")


LB = [SecDividend, SecIndustry, SecAdjFactor, BalanceSheet, Income, CashFlow, SState, SecSusp,
      ProfitExpress, SecRestricted, IndexCons, IndexWeightRange, FinIndicator, WindFinance, DailyIndicator]

JZ = [InstrumentInfo, SecTradeCal, ApiList, ApiParam]

OTHER = [DailyFactor, FxdayuFactor]

