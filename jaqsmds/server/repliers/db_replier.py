import pymongo
from jaqsmds.server.repliers.basic import RegularReplier, no_error
from jaqsmds.server.repliers.jset import lb_readers, jz_readers
from jaqsmds.server.repliers.factor import FactorReader
import logging
from datetime import datetime


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


class JsetReader(MongodbHandler):

    def __init__(self, client, lb=None, jz=None, factor=None):
        super(JsetReader, self).__init__(client)
        self.handlers = {}

        if lb:
            self.handlers.update(lb_readers(self.client[lb]))
        if jz:
            self.handlers.update(jz_readers(self.client[jz]))
        if factor:
            self.handlers['factor'] = FactorReader(self.client[factor])

    def receive(self, view, filter, fields, **kwargs):
        reader = self.handlers.get(view, None)
        if reader is not None:
            return reader.read(filter, fields)
        else:
            raise ValueError("Invalid view: %s" % view)


class DBReplier(RegularReplier):

    def __init__(self, host, jz="jz", lb='lb', factor="factor"):
        super().__init__()
        self.client = pymongo.MongoClient(host)
        self.jset = JsetReader(self.client, lb, jz, factor)
        self.methods["jset.query"] = self.jset.handle
