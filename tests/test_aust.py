from jaqsmds.server import auth
from redis import StrictRedis
from pymongo import MongoClient
from unittest import TestCase
from datautils.fxdayu import instance
from datautils.fxdayu import conf
import unittest


class TestAuth(TestCase):

    def setUp(self):
        self.client = "09380341$34234234"
        self.auth = auth
        self.auth.Auth.init(StrictRedis.from_url("redis://192.168.0.102/1"), MongoClient(port=37017)["test"]["auth"])
        self.login_request = {"params": {"username": "user1", "password": "Xinger520"}, "client": self.client}
        self.heartbeat = {"params": {}, "client": self.client}
        self.query = {"params": {"view": "factor",
                                 "filter": "start=20170101&end=20180101&symbol=000001,000002,600000",
                                 "fields": "PB,PE,ACCA"},
                      "client": self.client}
        self.valid = ['symbol', 'datetime', 'PB', 'PE']

    def test_login_success(self):
        reply = self.auth.login(self.login_request)
        self.assertEqual(reply["result"], 'username: user1')
        self.assertEqual(reply["error"]["error"], 0)

    def test_heartbeat(self):
        self.test_login_success()
        reply = self.auth.heartbeat(self.heartbeat)
        self.assertEqual(reply["error"]["error"], 0)

    def test_login_fail_username(self):
        self.login_request["params"]["username"] = "user2"
        reply = self.auth.login(self.login_request)
        self.assertEqual(reply["error"]["error"], -1000)

    def test_login_fail_password(self):
        self.login_request["params"]["password"] = "Xinge520"
        reply = self.auth.login(self.login_request)
        self.assertEqual(reply["error"]["error"], -1000)

    def test_heartbeat_fail(self):
        self.heartbeat["client"] = "09380341$34334234"
        reply = self.auth.heartbeat(self.heartbeat)
        self.assertEqual(reply["error"]["error"], -1000)


if __name__ == '__main__':
    unittest.main()