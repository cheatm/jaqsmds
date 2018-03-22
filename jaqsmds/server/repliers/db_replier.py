from jaqsmds.server.repliers.basic import RegularReplier
from jaqsmds.server.repliers.jsets import JsetHandler
from jaqsmds.server.repliers.daily import DailyHandler
from jaqsmds.server.repliers.jsi import JsiHandler
import pymongo


JSET = "jset.query"
JSD = "jsd.query"
JSI = "jsi.query"


class DBReplier(RegularReplier):

    def __init__(self, host, db_map):
        super().__init__()
        self.client = pymongo.MongoClient(host)
        self.db_map = db_map if db_map else {}
        self.handlers = {}

        for method, func in self.mapper:
            db = self.db_map.get(method, None)
            if db:
                handler = func(self, db)
                self.handlers[method] = handler
                self.methods[method] = handler.handle

    def init_jsd(self, db):
        jz = self.db_map.get(JSET, {}).get("jz", "jz")
        trade_cal = "%s.secTradeCal" % jz
        return DailyHandler(self.client, db, self.handlers[JSET]["lb.secAdjFactor"], trade_cal)

    def init_jset(self, dbs):
        return JsetHandler(self.client, **dbs)

    def init_jsi(self, db):
        return JsiHandler(self.client, db)

    mapper = (
        (JSET, init_jset),
        (JSD, init_jsd),
        (JSI, init_jsi)
    )