from jaqsmds.server.repliers.auth_replier import AuthJsetHandler
from jaqsmds.server import auth
from pymongo import MongoClient
from redis import StrictRedis
from datautils.fxdayu import instance
from datautils.fxdayu import conf
import pandas as pd
import unittest


class TestAuthJsetHandler(unittest.TestCase):

    def setUp(self):
        conf.MONGODB_URI = "192.168.0.102"
        conf.FACTOR = "factors"
        instance.init()
        auth.Auth.init(StrictRedis.from_url("redis://192.168.0.102/1"), MongoClient(port=37017)["test"]["auth"])
        self.client = "3420934802$123123123"
        self.login_request = {"params": {"username": "user1", "password": "Xinger520"}, "client": self.client}
        self.heartbeat = {"params": {}, "client": self.client}
        self.handler = AuthJsetHandler()
        self.query = {"params": {"view": "factor",
                                 "filter": "start=20170101&end=20180101&symbol=000001,000002,600000",
                                 "fields": "PB,PE,ACCA"},
                      "client": self.client}
        self.valid = ['symbol', 'datetime', 'PB', 'PE']
        self.login_success()

    def login_success(self):
        reply = auth.login(self.login_request)
        self.assertEqual(reply["result"], 'username: user1')
        self.assertEqual(reply["error"]["error"], 0)

    def test_factor(self):
        data = self.handler.handle(self.query)
        for key in data["result"]:
            self.assertTrue(key in self.valid)
        self.assertTrue("ACCA" in data["error"]["message"])
        frame = pd.DataFrame(data["result"])
        self.assertEqual(frame.shape, (732, 4))

    def test_factor_not_login(self):
        self.query["client"] = "23413435$32334666"
        data = self.handler.handle(self.query)
        self.assertEqual(data["error"]["error"], -1000)
        self.assertEqual(data["error"]["message"], "not login")