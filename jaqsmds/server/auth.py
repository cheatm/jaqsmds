from datautils.tools.logger import logger
from jaqsmds.server.repliers.basic import heartbeat as _heartbeat, logout as _logout


class BaseAuth(object):

    def login(self, client, user, password):
        pass

    def logout(self, client):
        pass

    def heartbeat(self, client):
        pass

    def permission(self, client):
        pass

    def permission_expire(self, name, values):
        pass


class Auth(BaseAuth):

    @classmethod
    def init(cls, redis, collection, expire=300):
        instance = cls(redis, collection, expire)
        globals()["auth"] = instance
        return instance

    def __init__(self, redis, collection, expire=300):
        self.redis = redis
        self.collection = collection
        self.expire = expire

    def find(self, user):
        return self.collection.find_one({"username": user})

    @logger("login", 1, 2)
    def login(self, client, user, password):
        doc = self.find(user)
        if isinstance(doc, dict):
            if doc["password"] == password:
                if self.permission_expire(client, doc["permission"]):
                    return True
        return False

    @logger("logout", 1)
    def logout(self, client):
        return self.redis.delete(client)

    @logger("heartbeat", 1, default=lambda: False, success="debug")
    def heartbeat(self, client):
        return self.redis.expire(client, self.expire)

    @logger("permission", 1, success="debug")
    def permission(self, client):
        pipeline = self.redis.pipeline()
        pipeline.keys(client)
        pipeline.expire(client, self.expire)
        pipeline.smembers(client)
        result = pipeline.execute()
        if len(result[0]):
            return result[-1]
        else:
            return None

    @logger("permission expire", 1, 2, success="warning")
    def permission_expire(self, name, values):
        pipeline = self.redis.pipeline()
        pipeline.sadd(name, *values)
        pipeline.expire(name, self.expire)
        result = pipeline.execute()
        return result[1]


auth = BaseAuth()


def init():
    from pymongo import MongoClient
    from redis import StrictRedis
    from jaqsmds.server import conf

    db, col = conf.AUTH.split(".", 1)
    Auth.init(
        StrictRedis.from_url(conf.REDIS_URL),
        MongoClient(conf.MONGODB_URI)[db][col],
        conf.AUTH_EXPIRE
    )


def not_login_error(dct):
    dct["error"] = {"error": -1000, "message": "not login"}
    return dct


def heartbeat(dct):
    client = dct.pop("client", None)
    if client is None:
        return not_login_error(dct)
    result = auth.heartbeat(client)
    reply = _heartbeat(dct)
    if result:
        return reply
    else:
        return not_login_error(dct)


def login(dct):
    params = dct.pop("params")
    username = params["username"]
    password = params["password"]
    result = auth.login(
        dct.pop("client"),
        username,
        password
    )
    if result:
        dct["result"] = "username: %s" % username
        dct["error"] = {"error": 0}
    else:
        dct["error"] = {"error": -1000,
                        "message": "login failure: Map(username -> %s, password -> %s)" % (username, password)}
    return dct


def logout(dct):
    auth.logout(dct.pop("client"))
    return _logout(dct)


def permission(dct):
    client = dct.pop("client")
    if client:
        return auth.permission(client)
    else:
        return None
