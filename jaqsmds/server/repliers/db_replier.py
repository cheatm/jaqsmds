import pymongo
from jaqsmds.server.repliers.basic import RegularReplier, no_error
import logging
from datetime import datetime
import pandas as pd


class MongodbHandler(object):

    def __init__(self, client):
        self.client = client

    def handle(self, dct):
        dct = dct.copy()
        logging.warning("Message: %s", dct)
        try:
            result = self.receive(**dct.pop("params"))
        except Exception as e:
            dct["error"] = {"error": -1, "message": str(e)}
            dct["result"] = {}
            logging.error('jset: %s', e)
        else:
            dct["result"] = result
            no_error(dct)

        dct["time"] = datetime.now().timestamp() * 1000
        return dct

    def receive(self, **kwargs):
        pass


def read(collection, _filter, projection):
    projection["_id"] = 0
    return pd.DataFrame(list(collection.find(_filter, projection)))


class DBReplier(RegularReplier):

    def __init__(self, host):
        super().__init__()
        from jaqsmds.server.repliers.jset_replier import JsetReplier
        self.client = pymongo.MongoClient(host)
        self.jset = JsetReplier(self.client)
        self.methods["jset.query"] = self.jset.handle