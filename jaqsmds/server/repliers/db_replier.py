from jaqsmds.server.repliers.basic import RegularReplier
from jaqsmds.server.repliers.jset import JsetHandler
from jaqsmds.server.repliers.daily import DailyReader
from six import string_types
import pymongo


mapper = {
    "jset.query": JsetHandler,
    "jsd.query": DailyReader
}


class DBReplier(RegularReplier):

    def __init__(self, host, db_map):
        super().__init__()
        self.client = pymongo.MongoClient(host)
        self.db_map = db_map if db_map else {}
        self.handlers = {}
        for method, cls in mapper.items():
            db = self.db_map.get(method, None)
            if isinstance(db, string_types):
                handler = cls(self.client, db)
            elif isinstance(db, dict):
                handler = cls(self.client, **db)
            else:
                continue
            self.handlers[method] = handler
            self.methods[method] = handler.handle