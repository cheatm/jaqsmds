from jaqsmds.server.repliers.basic import RegularReplier
from jaqsmds.server.repliers.jsets import JsetHandler
from jaqsmds.server.repliers.daily import DailyHandler
import pymongo


JSET = "jset.query"
JSD = "jsd.query"

mapper = {
    JSET: JsetHandler,
    JSD: DailyHandler
}


class DBReplier(RegularReplier):

    def __init__(self, host, db_map):
        super().__init__()
        self.client = pymongo.MongoClient(host)
        self.db_map = db_map if db_map else {}
        self.handlers = {}

        for method, func in self.mapper:
            db = self.db_map.get(method, None)
            if db:
                func(self, db)

    def init_jsd(self, db):
        handler = DailyHandler(self.client, db, self.handlers[JSET]["lb.secAdjFactor"])
        self.handlers[JSD] = handler
        self.methods[JSD] = handler.handle

    def init_jset(self, dbs):
        handler = JsetHandler(self.client, **dbs)
        self.handlers[JSET] = handler
        self.methods[JSET] = handler.handle

    mapper = ((JSET, init_jset), (JSD, init_jsd))