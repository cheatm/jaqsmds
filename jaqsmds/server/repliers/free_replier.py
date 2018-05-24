from jaqsmds.server.repliers.basic import RegularReplier
from jaqsmds.server.repliers.handlers import JsetHandler, JsdHandler, JsiHandler


class FreeReplier(RegularReplier):

    def __init__(self):
        super(FreeReplier, self).__init__()
        self.jset = JsetHandler()
        self.jsd = JsdHandler()
        self.jsi = JsiHandler()
        self.methods["jset.query"] = self.jset.handle
        self.methods["jsd.query"] = self.jsd.handle
        self.methods["jsi.query"] = self.jsi.handle


def tests():
    from datautils.fxdayu import conf
    from datautils.fxdayu import instance
    from jaqsmds import logger
    logger.init()

    conf.MONGODB_URI = "192.168.0.102"
    conf.FACTOR = "factors"
    instance.init()
    #
    replier = FreeReplier()

    # msg = {"method": "jsd.query",
    #        "params": {"symbol": "000001.SZ,000002.SZ", "begin_date": 20160101, "end_date": 20160131}}
    # result = replier.handle(msg)
    # print(result)
    #
    print(replier.jset.receive(**{'view': 'lb.secDailyIndicator', 'fields': 'net_assets,trade_date,pb,pcf_ncf,symbol', 'filter': 'symbol=000001.SZ,000063.SZ,600030.SH&start_date=20160406&end_date=20170601', 'orderby': 'trade_date'}))


if __name__ == '__main__':
    tests()