# encoding:utf-8
from itertools import chain


class QueryStructure(object):

    def __init__(
            self,
            view,
            compulsory,
            optional,
            fields,
            data_format="pandas",
            defaults=None
    ):
        self.view = view
        self.compulsory = compulsory
        self.optional = optional
        self.fields = fields
        self.data_format = data_format
        self.defaults = defaults if isinstance(defaults, dict) else {}

    def __call__(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    def query(self, *fields, **filters):
        self._check(filters)
        if len(fields):
            f = ",".join(fields)
        else:
            f = ",".join(self.fields)
            # f = ""
        return {
            "view": self.view,
            "filter": self._filter(**filters),
            "fields": f,
            "data_format": self.data_format
        }

    def _check(self, arguments):
        for key in self.compulsory:
            if key not in arguments:
                try:
                    default = self.defaults[key]
                except KeyError:
                    raise ValueError("'%s' is compulsory but not in arguments or defaults." % key)
                else:
                    arguments[key] = default

    @staticmethod
    def _filter(**kwargs):
        return "&".join(["{}={}".format(*item) for item in kwargs.items()])

    @property
    def arguments(self):
        return list(chain(self.compulsory, self.optional))


SYMBOL = ("symbol",)
DATE_RANGE = ("start_date", "end_date")


# 证券基础信息表
InstrumentInfo = QueryStructure(
    view="jz.instrumentInfo",
    compulsory=["symbol"],
    optional=["inst_type", "status"],
    fields=[
        "inst_type", "market", "symbol", "name", "list_date", "delist_date",
        "cnspell", "currency", "status", "buylot", "selllot", "pricetick",
        "product", "underlying", "multiplier"
    ],
    defaults={"symbol": ""}
)


# 交易日历表
SecTradeCal = QueryStructure(
    view="jz.secTradeCal",
    compulsory=["start_date", "end_date"],
    optional=[],
    fields=["trade_date", "istradeday", "isweekday", "isweekend", "isholiday"],
    defaults={"start_date": 0, "end_date": 99999999}
)


# 分配除权信息表
SecDividend = QueryStructure(
    view="lb.secDividend",
    compulsory=["symbol"],
    optional=["start_date", "end_date"],
    fields=["symbol", "ann_date", "end_date", "process_stauts", "publish_date", "record_date",
            "exdiv_date", "cash", "cash_tax", "share_ratio", "share_trans_ratio", "cashpay_date",
            "bonus_list_date"],
)


# 复权因子表
SecAdjFactor = QueryStructure(
    view="lb.secAdjFactor",
    compulsory=["symbol"],
    optional=["start_date", "end_date"],
    fields=["symbol", "trade_date", "adjust_factor"]
)


# 停复牌信息表
SecSusp = QueryStructure(
    view="lb.secSusp",
    compulsory=[],
    optional=["symbol", "start_date", "end_date"],
    fields=['symbol', 'ann_date', 'susp_date', 'susp_time', 'resu_date', 'resu_time', 'susp_reason']
)


# 行业分类表
SecIndustry = QueryStructure(
    view="lb.secIndustry",
    compulsory=['symbol', "industry_src"],
    optional=['industry1_name', 'industry2_name', 'industry3_name', 'industry4_name'],
    fields=['symbol', 'industry_src', 'in_date', 'out_date', 'is_new',
            'industry1_code', 'industry1_name', 'industry2_code', 'industry2_name',
            'industry3_code', 'industry3_name', 'industry4_code', 'industry4_name'],
    defaults={"industry_src": "中证指数有限公司"}
)


# 常量参数表
SysConstants = QueryStructure(
    view='jz.sysConstants',
    compulsory=["code_type"],
    optional=[],
    fields=['code_type', 'type_name', 'code', 'value']
)


# 日行情估值表
SecDailyIndicator = QueryStructure(
    view="lb.secDailyIndicator",
    compulsory=['symbol'],
    optional=DATE_RANGE,
    fields=['symbol', 'trade_date', 'total_mv', 'float_mv', 'pe', 'pb_new', 'pe_ttm',  'pcf_ocf',
            'pcf_ocfttm', 'pcf_ncf', 'pcf_ncfttm', 'ps', 'ps_ttm',  'turnoverratio', 'freeturnover',
            'total_share', 'float_share', 'close', 'price_div_dps', 'free_share', 'profit_ttm', 'profit_lyr',
            'net_assets', 'cash_flows_oper_act_ttm', 'cash_flows_oper_act_lyr', 'operrev_ttm', 'operrev_lyr',
            'limit_status']
)


# 资产负债表
BalanceSheet = QueryStructure(
    view="lb.balanceSheet",
    compulsory=[],
    optional=['symbol', 'start_date', 'end_date', 'comp_type_code', 'start_actdate', 'end_actdate',
              'start_reportdate', 'start_reportdate', 'report_type', 'update_flag'],
    fields=['symbol', 'ann_date', 'comp_type_code', 'act_ann_date', 'report_date', 'report_type',
            'currency', 'monetary_cap', 'tradable_assets', 'notes_rcv', 'acct_rcv', 'other_rcv',
            'pre_pay', 'dvd_rcv', 'int_rcv', 'inventories', 'consumptive_assets', 'deferred_exp',
            'noncur_assets_due_1y', 'settle_rsrv', 'loans_to_banks', 'prem_rcv', 'rcv_from_reinsurer',
            'rcv_from_ceded_insur_cont_rsrv', 'red_monetary_cap_for_sale', 'other_cur_assets',
            'tot_cur_assets', 'fin_assets_avail_for_sale', 'held_to_mty_invest', 'long_term_eqy_invest',
            'invest_real_estate', 'time_deposits', 'other_assets', 'long_term_rec', 'fix_assets',
            'const_in_prog', 'proj_matl', 'fix_assets_disp', 'productive_bio_assets', 'oil_and_natural_gas_assets',
            'intang_assets', 'r_and_d_costs', 'goodwill', 'long_term_deferred_exp', 'deferred_tax_assets',
            'loans_and_adv_granted', 'oth_non_cur_assets', 'tot_non_cur_assets', 'cash_deposits_central_bank',
            'asset_dep_oth_banks_fin_inst', 'precious_metals', 'derivative_fin_assets', 'agency_bus_assets',
            'subr_rec', 'rcv_ceded_unearned_prem_rsrv', 'rcv_ceded_claim_rsrv', 'rcv_ceded_life_insur_rsrv',
            'rcv_ceded_lt_health_insur_rsrv', 'mrgn_paid', 'insured_pledge_loan', 'cap_mrgn_paid',
            'independent_acct_assets', 'clients_cap_deposit', 'clients_rsrv_settle', 'incl_seat_fees_exchange',
            'rcv_invest', 'tot_assets', 'st_borrow', 'borrow_central_bank', 'deposit_received_ib_deposits',
            'loans_oth_banks', 'tradable_fin_liab', 'notes_payable', 'acct_payable', 'adv_from_cust',
            'fund_sales_fin_assets_rp', 'handling_charges_comm_payable', 'empl_ben_payable',
            'taxes_surcharges_payable', 'int_payable', 'dvd_payable', 'other_payable', 'acc_exp',
            'deferred_inc', 'st_bonds_payable', 'payable_to_reinsurer', 'rsrv_insur_cont', 'acting_trading_sec',
            'acting_uw_sec', 'non_cur_liab_due_within_1y', 'other_cur_liab', 'tot_cur_liab', 'lt_borrow',
            'bonds_payable', 'lt_payable', 'specific_item_payable', 'provisions', 'deferred_tax_liab',
            'deferred_inc_non_cur_liab', 'other_non_cur_liab', 'tot_non_cur_liab', 'liab_dep_other_banks_inst',
            'derivative_fin_liab', 'cust_bank_dep', 'agency_bus_liab', 'other_liab', 'prem_received_adv',
            'deposit_received', 'insured_deposit_invest', 'unearned_prem_rsrv', 'out_loss_rsrv',
            'life_insur_rsrv', 'lt_health_insur_v', 'independent_acct_liab', 'incl_pledge_loan',
            'claims_payable', 'dvd_payable_insured', 'total_liab', 'capital_stk', 'capital_reser',
            'special_rsrv', 'surplus_rsrv', 'undistributed_profit', 'less_tsy_stk', 'prov_nom_risks',
            'cnvd_diff_foreign_curr_stat', 'unconfirmed_invest_loss', 'minority_int', 'tot_shrhldr_eqy_excl_min_int',
            'tot_shrhldr_eqy_incl_min_int', 'tot_liab_shrhldr_eqy', 'spe_cur_assets_diff', 'tot_cur_assets_diff',
            'spe_non_cur_assets_diff', 'tot_non_cur_assets_diff', 'spe_bal_assets_diff', 'tot_bal_assets_diff',
            'spe_cur_liab_diff', 'tot_cur_liab_diff', 'spe_non_cur_liab_diff', 'tot_non_cur_liab_diff',
            'spe_bal_liab_diff', 'tot_bal_liab_diff', 'spe_bal_shrhldr_eqy_diff', 'tot_bal_shrhldr_eqy_diff',
            'spe_bal_liab_eqy_diff', 'tot_bal_liab_eqy_diff', 'lt_payroll_payable', 'other_comp_income',
            'other_equity_tools', 'other_equity_tools_p_shr', 'lending_funds', 'accounts_receivable',
            'st_financing_payable', 'payables', 'update_flag']
)


# 利润表
Income = QueryStructure(
    view="lb.income",
    compulsory=["symbol"],
    optional=['start_date', 'end_date', 'comp_type_code', 'start_actdate', 'end_actdate',
              'start_reportdate', 'start_reportdate', 'report_type', 'update_flag'],
    fields=['symbol', 'ann_date', 'comp_type_code', 'act_ann_date', 'report_date', 'report_type',
            'currency', 'total_oper_rev', 'oper_rev', 'int_income', 'net_int_income', 'insur_prem_unearned',
            'handling_chrg_income', 'net_handling_chrg_income', 'net_inc_other_ops', 'plus_net_inc_other_bus',
            'prem_income', 'less_ceded_out_prem', 'chg_unearned_prem_res', 'incl_reinsurance_prem_inc',
            'net_inc_sec_trading_brok_bus', 'net_inc_sec_uw_bus', 'net_inc_ec_asset_mgmt_bus', 'other_bus_income',
            'plus_net_gain_chg_fv', 'plus_net_invest_inc', 'incl_inc_invest_assoc_jv_entp', 'plus_net_gain_fx_trans',
            'tot_oper_cost', 'less_oper_cost', 'less_int_exp', 'less_handling_chrg_comm_exp',
            'less_taxes_surcharges_ops', 'less_selling_dist_exp', 'less_gerl_admin_exp', 'less_fin_exp',
            'less_impair_loss_assets', 'prepay_surr', 'tot_claim_exp', 'chg_insur_cont_rsrv', 'dvd_exp_insured',
            'reinsurance_exp', 'oper_exp', 'less_claim_recb_reinsurer', 'less_ins_rsrv_recb_reinsurer',
            'less_exp_recb_reinsurer', 'other_bus_cost', 'oper_profit', 'plus_non_oper_rev', 'less_non_oper_exp',
            'il_net_loss_disp_noncur_asset', 'tot_profit', 'inc_tax', 'unconfirmed_invest_loss',
            'net_profit_incl_min_int_inc', 'net_profit_excl_min_int_inc', 'minority_int_inc', 'other_compreh_inc',
            'tot_compreh_inc', 'tot_compreh_inc_parent_comp', 'tot_compreh_inc_min_shrhldr', 'ebit', 'ebitda',
            'net_profit_after_ded_nr_lp', 'net_profit_under_intl_acc_sta', 's_fa_eps_basic', 's_fa_eps_diluted',
            'insurance_expense', 'spe_bal_oper_profit', 'tot_bal_oper_profit', 'spe_bal_tot_profit',
            'tot_bal_tot_profit', 'spe_bal_net_profit', 'tot_bal_net_profit', 'undistributed_profit',
            'adjlossgain_prevyear', 'transfer_from_surplusreserve', 'transfer_from_housingimprest', 'update_flag'
            'transfer_from_others', 'distributable_profit', 'withdr_legalsurplus', 'withdr_legalpubwelfunds',
            'workers_welfare', 'withdr_buzexpwelfare', 'withdr_reservefund', 'distributable_profit_shrhder',
            'prfshare_dvd_payable', 'withdr_othersurpreserve', 'comshare_dvd_payable', 'capitalized_comstock_div']
)


# 现金流量表
CashFlow = QueryStructure(
    view="lb.cashFlow",
    compulsory=["symbol"],
    optional=['start_date', 'end_date', 'comp_type_code', 'start_actdate', 'end_actdate',
              'start_reportdate', 'start_reportdate', 'report_type', 'update_flag'],
    fields=['symbol', 'ann_date', 'comp_type_code', 'act_ann_date', 'report_date', 'report_type', 'currency',
            'cash_recp_sg_and_rs', 'recp_tax_rends', 'net_incr_dep_cob', 'net_incr_loans_central_bank',
            'net_incr_fund_borr_ofi', 'cash_recp_prem_orig_inco', 'net_incr_insured_dep',
            'net_cash_received_reinsu_bus', 'net_incr_disp_tfa', 'net_incr_int_handling_chrg', 'net_incr_disp_faas',
            'net_incr_loans_other_bank', 'net_incr_repurch_bus_fund', 'other_cash_recp_ral_oper_act',
            'stot_cash_inflows_oper_act', 'cash_pay_goods_purch_serv_rec', 'cash_pay_beh_empl', 'pay_all_typ_tax',
            'net_incr_clients_loan_adv', 'net_incr_dep_cbob', 'cash_pay_claims_orig_inco', 'handling_chrg_paid',
            'comm_insur_plcy_paid', 'other_cash_pay_ral_oper_act', 'stot_cash_outflows_oper_act',
            'net_cash_flows_oper_act', 'cash_recp_disp_withdrwl_invest', 'cash_recp_return_invest',
            'net_cash_recp_disp_fiolta', 'net_cash_recp_disp_sobu', 'other_cash_recp_ral_inv_act',
            'stot_cash_inflows_inv_act', 'cash_pay_acq_const_fiolta', 'cash_paid_invest', 'net_cash_pay_aquis_sobu',
            'other_cash_pay_ral_inv_act', 'net_incr_pledge_loan', 'stot_cash_outflows_inv_act',
            'net_cash_flows_inv_act', 'cash_recp_cap_contrib', 'incl_cash_rec_saims', 'cash_recp_borrow',
            'proc_issue_bonds', 'other_cash_recp_ral_fnc_act', 'stot_cash_inflows_fnc_act', 'cash_prepay_amt_borr',
            'cash_pay_dist_dpcp_int_exp', 'incl_dvd_profit_paid_sc_ms', 'other_cash_pay_ral_fnc_act',
            'stot_cash_outflows_fnc_act', 'net_cash_flows_fnc_act', 'eff_fx_flu_cash', 'net_incr_cash_cash_equ',
            'cash_cash_equ_beg_period', 'cash_cash_equ_end_period', 'net_profit', 'unconfirmed_invest_loss',
            'plus_prov_depr_assets', 'depr_fa_coga_dpba', 'amort_intang_assets', 'amort_lt_deferred_exp',
            'decr_deferred_exp', 'incr_acc_exp', 'loss_disp_fiolta', 'loss_scr_fa', 'loss_fv_chg', 'fin_exp',
            'invest_loss', 'decr_deferred_inc_tax_assets', 'incr_deferred_inc_tax_liab', 'decr_inventories',
            'decr_oper_payable', 'incr_oper_payable', 'others', 'im_net_cash_flows_oper_act', 'conv_debt_into_cap',
            'conv_corp_bonds_due_within_1y', 'fa_fnc_leases', 'end_bal_cash', 'less_beg_bal_cash',
            'plus_end_bal_cash_equ', 'less_beg_bal_cash_equ', 'im_net_incr_cash_cash_equ', 'free_cash_flow',
            'spe_bal_cash_inflows_oper', 'tot_bal_cash_inflows_oper', 'spe_bal_cash_outflows_oper',
            'tot_bal_cash_outflows_oper', 'tot_bal_netcash_outflows_oper', 'spe_bal_cash_inflows_inv',
            'tot_bal_cash_inflows_inv', 'spe_bal_cash_outflows_inv', 'tot_bal_cash_outflows_inv',
            'tot_bal_netcash_outflows_inv', 'spe_bal_cash_inflows_fnc', 'tot_bal_cash_inflows_fnc',
            'spe_bal_cash_outflows_fnc', 'tot_bal_cash_outflows_fnc', 'tot_bal_netcash_outflows_fnc',
            'spe_bal_netcash_inc', 'tot_bal_netcash_inc', 'spe_bal_netcash_equ_undir', 'tot_bal_netcash_equ_undir',
            'spe_bal_netcash_inc_undir', 'spe_bal_netcash_inc_undir', 'update_flag']
)


# 业绩快报
ProfitExpress = QueryStructure(
    view="lb.profitExpress",
    compulsory=SYMBOL,
    optional=['start_anndate', 'end_anndate', 'start_reportdate', 'end_reportdate'],
    fields=['symbol', 'ann_date', 'report_date', 'oper_rev', 'oper_profit', 'total_profit', 'net_profit_int_inc',
            'total_assets', 'tot_shrhldr_int', 'eps_diluted', 'roe_diluted', 'is_audit', 'yoy_int_inc']
)


# 限售股解禁表
SecRestricted = QueryStructure(
    view="lb.secRestricted",
    compulsory=[],
    optional=["symbol", "start_date", "end_date"],
    fields=['symbol', 'list_date', 'lifted_reason', 'lifted_shares', 'lifted_ratio'],
)


# 指数基本信息表
IndexCons = QueryStructure(
    view="lb.indexCons",
    compulsory=["index_code", "start_date", "end_date"],
    optional=[],
    fields=['index_code', "symbol", "in_date", "out_date", "is_new"],
    defaults={'start_date': 0, "end_date": 99999999}
)


# 公募基金净值表
MFNav = QueryStructure(
    view="lb.mfNav",
    compulsory=[],
    optional=['symbol', 'start_date', 'end_date', 'start_pdate', 'end_pdate', 'update_flag'],
    fields=['symbol', 'ann_date', 'price_date', 'nav', 'nav_accumulated', 'div_accumulated', 'adj_factor', 'currency',
            'netasset', 'if_mergedshare', 'netasset_total', 'nav_adjusted', 'update_flag']
)


# 基金分红表
MFDividend = QueryStructure(
    view="lb.mfDividend",
    compulsory=[],
    optional=['symbol', 'start_date', 'end_date', "update_flag"],
    fields=['symbol', 'ann_date', 'ebch_date', 'div_progress', 'cash_dvd', 'currency', 'record_date', 'ex_date',
            'div_edexdate', 'pay_date', 'div_paydate', 'div_impdate', 'sh_bch_y', 'bch_unit', 'eapr', 'exdiv_date',
            'eapr_amount', 'reinv_bch_date', 'reinv_toac_date', 'reinv_redeem_date', 'div_object', 'div_ipaydt',
            'update_flag']
)


# 基金投资组合表
MFPortfolio = QueryStructure(
    view="lb.mfPortfolio",
    compulsory=SYMBOL,
    optional=['start_date', 'end_date', "update_flag"],
    fields=['symbol', 'ann_date', 'prt_enddate', 'currency', 's_symbol', 'stk_value', 'stk_quantity', 'stk_valuetonav',
            'posstk_value', 'posstk_quantity', 'posstkto_nav', 'passtke_value', 'passtk_quantity', 'passtkto_nav',
            'stock_per', 'float_shr_per', 'update_flag']
)


# 基金持有债券组合
MFBondPortfolio = QueryStructure(
    view="lb.mfBondPortfolio",
    compulsory=[],
    optional=['symbol', 'start_date', 'end_date', "update_flag"],
    fields=['symbol', 'prt_enddate', 'currency', 'bond_code', 'bond_value',
            'bond_quantity', 'bond_valueto_nav', 'update_flag']
)


FinIndicator = QueryStructure(
    view="lb.finIndicator",
    compulsory=[],
    optional=["symbol", "start_date", "end_date"],
    fields=["extraordinary", "deductedprofit", "grossmargin", "operateincome", "investincome", "stmnote_finexp",
             "stm_is", "ebit_daily", "ebitda""fcff", "fcfe", "exinterestdebt_current", "exinterestdebt_noncurrent",
             "interestdebt",
             "netdebt", "tangibleasset", "workingcapital", "networkingcapital", "investcapital", "retainedearnings",
             "eps_basic_daily",  # TODO eps_basic
             "eps_diluted", "eps_diluted2", "bps", "ocfps", "grps", "orps", "surpluscapitalps", "surplusreserveps",
             "undistributedps",
             "retainedps", "cfps", "ebitps", "fcffps", "fcfeps", "netprofitmargin", "grossprofitmargin", "cogstosales",
             "expensetosales", "profittogr", "saleexpensetogr", "adminexpensetogr", "finaexpensetogr", "impairtogr_ttm",
             "gctogr", "optogr", "ebittogr", "roe", "roe_deducted", "roa2", "roa", "roic", "roe_yearly", "roa2_yearly",
             "roe_avg",
             "operateincometoebt", "investincometoebt", "nonoperateprofittoebt", "taxtoebt", "deductedprofittoprofit",
             "salescashintoor",
             "ocftoor", "ocftooperateincome", "capitalizedtoda", "debttoassets", "assetstoequity",
             "dupont_assetstoequity",
             "catoassets", "ncatoassets", "tangibleassetstoassets", "intdebttototalcap", "equitytototalcapital",
             "currentdebttodebt",
             "longdebtodebt", "current", "quick", "cashratio", "ocftoshortdebt", "debttoequity", "equitytodebt",
             "equitytointerestdebt", "tangibleassettodebt", "tangassettointdebt", "tangibleassettonetdebt", "ocftodebt",
             "ocftointerestdebt", "ocftonetdebt", "ebittointerest", "longdebttoworkingcapital", "ebitdatodebt",
             "turndays",
             "invturndays", "arturndays", "invturn", "arturn", "caturn", "faturn", "assetsturn", "roa_yearly",
             "dupont_roa",
             "s_stm_bs", "prefinexpense_opprofit", "nonopprofit", "optoebt", "noptoebt", "ocftoprofit", "cashtoliqdebt",
             "cashtoliqdebtwithinterest", "optoliqdebt", "optodebt", "roic_yearly", "tot_faturn", "profittoop",
             "qfa_operateincome",
             "qfa_investincome", "qfa_deductedprofit", "qfa_eps", "qfa_netprofitmargin", "qfa_grossprofitmargin",
             "qfa_expensetosales",
             "qfa_profittogr", "qfa_saleexpensetogr", "qfa_adminexpensetogr", "qfa_finaexpensetogr",
             "qfa_impairtogr_ttm",
             "qfa_gctogr", "qfa_optogr", "qfa_roe", "qfa_roe_deducted", "qfa_roa", "qfa_operateincometoebt",
             "qfa_investincometoebt",
             "qfa_deductedprofittoprofit", "qfa_salescashintoor", "qfa_ocftosales", "qfa_ocftoor", "yoyeps_basic",
             "yoyeps_diluted",
             "yoyocfps", "yoyop", "yoyebt", "yoynetprofit", "yoynetprofit_deducted", "yoyocf", "yoyroe", "yoybps",
             "yoyassets",
             "yoyequity", "yoy_tr", "yoy_or", "qfa_yoygr", "qfa_cgrgr", "qfa_yoysales", "qfa_cgrsales", "qfa_yoyop",
             "qfa_cgrop",
             "qfa_yoyprofit", "qfa_cgrprofit", "qfa_yoynetprofit", "qfa_cgrnetprofit", "yoy_equity", "rd_expense",
             "waa_roe"]
)

def dct(s):
    return {"view": s.view, "compulsory": ", ".join(s.compulsory), "optional": ", ".join(s.optional),
            "fields": ",".join(s.fields)}
