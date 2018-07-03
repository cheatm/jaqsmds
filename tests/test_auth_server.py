from jaqs.data import DataApi
import unittest


class TestAuthServer(unittest.TestCase):

    def setUp(self):
        self.api = DataApi("tcp://120.78.130.50:8910")
        # self.api = DataApi("tcp://localhost:23000")
        self.user1 = {"username": "guojin", "password": "guojin"}
        self.user2 = {"username": "user2", "password": "Xinger520"}
        self.query = {"view": "fxdayu.factor",
                      "filter": "start_date=20140101&end_date=20180101&symbol=000001,000002,600000,600036",
                      "fields": "E010030D,E010083D"}

    def tearDown(self):
        self.api.logout()

    def login_user1(self):
        return self.api.login(**self.user1)

    def login_user2(self):
        return self.api.login(**self.user2)

    def test_factor(self):
        self.login_user1()
        data, msg = self.api.query(**self.query)
        # self.assertEqual(msg, "0,Fields out of restrict: ACCA")
        print(data, msg)
        # self.assertEqual(data.shape, (3908, 4))

    def test_factor_wrong_user(self):
        result = self.login_user2()
        self.assertTrue(result, (None, '-1000,login failure: Map(username -> user2, password -> Xinger520)'))
        data, msg = self.api.query(**self.query)
        self.assertTrue(data is None)
        self.assertEqual(msg, "-1000,login failure: Map(username -> user2, password -> Xinger520)")

    def test_factor_fields_not_permitted(self):
        self.login_user1()
        self.query["fields"] = "ACCA,AD"
        data, msg = self.api.query(**self.query)
        self.assertTrue(data is None)
        self.assertTrue("-1,Fields out of restrict" in msg)


def main():
    unittest.main()

if __name__ == '__main__':
    main()