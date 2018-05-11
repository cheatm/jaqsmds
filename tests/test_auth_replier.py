from jaqsmds.server import auth
from redis import StrictRedis
from pymongo import MongoClient
from datautils.fxdayu import instance
from datautils.fxdayu import conf
from jaqsmds.server.repliers.auth_replier import AuthReplier
import pandas as pd
import unittest


class TestAuthReplier(unittest.TestCase):

    def setUp(self):
        self.client = "09380341$34234234"
        self.login_request = {"params": {"username": "user1", "password": "Xinger520"},
                              "client": self.client,
                              "method": "auth.login"}
        self.heartbeat = {"params": {},
                          "client": self.client,
                          "method": ".sys.heartbeat"}
        self.query = {"params": {"view": "factor",
                                 "filter": "start=20170101&end=20180101&symbol=000001,000002,600000",
                                 "fields": "PB,PE,ACCA"},
                      "method": "jset.query",
                      "client": self.client}
        self.valid = ['symbol', 'datetime', 'PB', 'PE']
        auth.Auth.init(StrictRedis.from_url("redis://192.168.0.102/1"), MongoClient(port=37017)["test"]["auth"])
        conf.MONGODB_URI = "192.168.0.102"
        instance.init()
        self.replier = AuthReplier()
        self.replier.handle(self.login_request)

    def test_factor(self):
        data = self.replier.handle(self.query)
        for key in data["result"]:
            self.assertTrue(key in self.valid)
        self.assertTrue("ACCA" in data["error"]["message"])
        frame = pd.DataFrame(data["result"])
        self.assertEqual(frame.shape, (732, 4))

    def test_wrong_method(self):
        self.query["method"] = "jset.q"
        data = self.replier.handle(self.query)
        self.assertTrue('no such method: jset.q' in data["error"]['message'].args)

    def test_wrong_view(self):
        self.query["params"]["view"] = "factors"
        data = self.replier.handle(self.query)
        self.assertTrue('No such view: factors' in data["error"]['message'].args)
