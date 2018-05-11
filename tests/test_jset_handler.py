from unittest import TestCase
from jaqsmds.server.repliers.instance_jset import JsetHandler
from datautils.fxdayu import instance
from datautils.fxdayu import conf


class TestJsetHandler(TestCase):

    def setUp(self):
        conf.MONGODB_URI = "192.168.0.102"
        instance.init()
        self.handler = JsetHandler()

    def test_instrumentInfo(self):
        data = self.handler.receive("jz.instrumentInfo", "inst_type=1&market=SZ,SH", "")
        self.assertEqual(data.shape[1], 14)

    def test_tradeCal(self):
        data = self.handler.receive("jz.secTradeCal", "start_date=20180101&end_date=20180501", "")
        self.assertEqual(data.shape, (77, 2))

    def test_apiList(self):
        data = self.handler.receive("help.apiList", "api=jz.instrumentInfo,lb.secIndustry,lb.secDividend", "")
        self.assertEqual(data.shape, (3, 3))

    def test_apiParam(self):
        data = self.handler.receive("help.apiParam", "api=jz.instrumentInfo,lb.secIndustry,lb.secDividend", "")
        self.assertEqual(data.shape, (59, 7))

    def test_balanceSheet(self):
        data = self.handler.receive('lb.balanceSheet',
                                    "symbol=000001.SZ,600000.SH&start_date=20160101&end_date=20180101",
                                    "")

        self.assertEqual(data.shape, (27, 8))

    def test_cashFlow(self):
        data = self.handler.receive('lb.cashFlow',
                                    "symbol=000001.SZ,600000.SH&start_date=20160101&end_date=20180101",
                                    "")
        self.assertEqual(data.shape, (92, 21))

    def test_income(self):
        data = self.handler.receive('lb.income',
                                    "symbol=000001.SZ,600000.SH&start_date=20160101&end_date=20180101",
                                    "")
        self.assertEqual(data.shape, (90, 12))

    def test_indexCons(self):
        data = self.handler.receive("lb.indexCons", "index_code=000300.SH&start_date=20170101&end_date=20180101", "")
        self.assertTrue(len(data)>=300)

    def test_indexWeightRange(self):
        data = self.handler.receive("lb.indexWeightRange", "index_code=000016.SH&start_date=20170101", "")
        print(data.sort_values("trade_date"))

    def test_profitExpress(self):
        data = self.handler.receive("lb.profitExpress", "start_anndate=20170101", "")
        print(data)

    def test_factor(self):
        data = self.handler.receive("factor", "start=20180101&symbol=000001,000002,600000", "PB,PE,ACCA")
        print(data)

    