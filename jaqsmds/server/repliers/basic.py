# encoding:utf-8
from datetime import datetime
import logging

from jaqs.data.dataapi.jrpc_py import _unpack_msgpack_snappy, _pack_msgpack_snappy


class Replier(object):

    def __init__(self):
        self.methods = {}

    def handle(self, request):
        message = _unpack_msgpack_snappy(request)
        try:
            reply = self.on_message(message)
        except:
            reply = self.on_message_error(message)

        return _pack_msgpack_snappy(reply)

    def on_message(self, message):
        return self.methods[message['method']](message)

    def on_message_error(self, message):
        return message


def no_error(dct):
    dct["error"] = {"error": 0}


def heartbeat(dct):
    res = dct.copy()
    res.pop("params")
    now_timestamp = datetime.now().timestamp()
    res["result"] = {"time": now_timestamp, "sub_hash": ""}
    no_error(dct)
    res['time'] = now_timestamp * 1000
    return res


def login(dct):
    res = dct.copy()
    params = res.pop("params")
    res["result"] = "username: %s" % params["username"]
    no_error(res)
    res['time'] = datetime.now().timestamp()
    logging.warning("login: %s" % params)
    return res


class RegularReplier(Replier):

    no_error = no_error

    def __init__(self):
        super().__init__()
        self.methods[".sys.heartbeat"] = heartbeat
        self.methods["auth.login"] = login