# encoding:utf-8
from jaqsmds.collector.structure import *
from enum import Enum
from collections import Iterable
import six


class EnvData(Enum):

    SYMBOL = "symbol"
    SYMBOL_LIST = "symbol_list"
    INDEX_CODE = "index_code"


class Table2D(object):

    def __init__(self, qs, db=None, index=None, arguments=None):
        self.qs = qs
        self.db = db if isinstance(db, six.string_types) else qs.view
        self.index = list(index) if isinstance(index, Iterable) else []
        self.arguments = arguments if isinstance(arguments, dict) else {}


class Table3D(object):

    def __init__(self, qs, db, index=None, iters=None):
        self.qs = qs
        self.db = db
        self.index = list(index) if isinstance(index, Iterable) else []
        self.iters = iters if isinstance(iters, dict) else {}


InstrumentInfoTable = Table2D(
    qs=InstrumentInfo,
    index=InstrumentInfo.arguments
)


SecTradeCalTable = Table2D(
    qs=SecTradeCal,
    index=["trade_date"]
)


SecDividendTable = Table2D(
    qs=SecDividend,
    index=["symbol", "exdiv_date"],
    arguments={"symbol": EnvData.SYMBOL}
)


SecAdjFactorTable = Table3D(
    qs=SecAdjFactor,
    db="SecAdjFactor",
    index=["trade_date"],
    iters=("symbol", EnvData.SYMBOL_LIST)
)


SecSuspTable = Table2D(
    qs=SecSusp,
    index=["symbol", "susp_date", "susp_date"]
)


SecIndustryTable = Table2D(
    qs=SecIndustry,
    index=SecIndustry.arguments,
    arguments={"symbol": EnvData.SYMBOL}
)


SysConstantsTable = Table2D(
    qs=SysConstants,
    index=SysConstants.arguments
)


SecDailyIndicatorTable = Table3D(
    qs=SecDailyIndicator,
    db="SecDailyIndicator",
    index=["trade_date"],
    iters=("symbol", EnvData.SYMBOL_LIST)
)


BalanceSheetTable = Table3D(
    qs=BalanceSheet,
    db="BalanceSheet",
    index=["symbol", "ann_date", 'comp_type_code', 'act_ann_date', 'report_date', 'report_type', "update_flag"],
    iters=("symbol", EnvData.SYMBOL_LIST)
)


IncomeTable = Table3D(
    qs=Income,
    db="Income",
    index=["symbol", 'ann_date', 'comp_type_code', 'act_ann_date', 'report_date', 'report_type', "update_flag"],
    iters=("symbol", EnvData.SYMBOL_LIST)
)


CashFlowTable = Table3D(
    qs=CashFlow,
    db="CashFlow",
    index=["symbol", 'ann_date', 'comp_type_code', 'act_ann_date', 'report_date', 'report_type', "update_flag"],
    iters=("symbol", EnvData.SYMBOL_LIST)
)


ProfitExpressTable = Table2D(
    qs=ProfitExpress,
    index=['symbol', 'ann_date', 'report_date'],
    arguments={"symbol", EnvData.SYMBOL}
)


SecRestrictedTable = Table2D(
    qs=SecRestricted,
    index=["symbol", 'list_date'],
)


IndexConsTable = Table2D(
    qs=IndexCons,
    index=["in_date", "index_code", "out_date", "symbol"],
    arguments={"index_code": EnvData.SYMBOL}
)


MFNavTable = Table3D(
    qs=MFNav,
    db="MFNav",
    index=['symbol', 'ann_date', 'price_date'],
    iters=("symbol", EnvData.SYMBOL)
)


MFDividendTable = Table3D(
    qs=MFDividend,
    db="MFDividend",
    index=["symbol", "ann_date"]
)


MFPortfolioTable = Table3D(
    qs=MFPortfolio,
    db="MFPortfolio",
    index=["symbol", "ann_date"]
)


MFBondPortfolioTable = Table3D(
    qs=MFBondPortfolio,
    db="MFBondPortfolio",
    index=["symbol", "prt_enddate"]
)

TABLES = (InstrumentInfoTable, CashFlowTable, IncomeTable, SecSuspTable, BalanceSheetTable, IndexConsTable, MFNavTable,
          ProfitExpressTable, SecDividendTable, SecAdjFactorTable, SecTradeCalTable, SecIndustryTable, MFDividendTable,
          SecRestrictedTable, SysConstantsTable, SecDailyIndicatorTable, MFPortfolioTable, MFBondPortfolioTable)


VIEW_MAP = {table.qs.view: table for table in TABLES}


__all__ = ["EnvData", "Table2D", "Table3D", "VIEW_MAP", "InstrumentInfoTable", "CashFlowTable", "IncomeTable", "SecSuspTable",
           "BalanceSheetTable", "IndexConsTable", "MFNavTable", "ProfitExpressTable", "SecDividendTable",
           "SecAdjFactorTable", "SecTradeCalTable", "SecIndustryTable", "SecRestrictedTable", "SysConstantsTable",
           "SecDailyIndicatorTable", "MFPortfolioTable", "MFBondPortfolioTable", "MFDividendTable"]