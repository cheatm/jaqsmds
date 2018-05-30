# encoding:utf-8
from datetime import datetime
import time
import logging

from jaqs.data.dataapi.jrpc_py import _unpack_msgpack_snappy, _pack_msgpack_snappy


class Replier(object):

    def __init__(self):
        self.methods = {}

    def handle(self, message):
        try:
            reply = self.on_message(message)
        except Exception as e:
            logging.error("message | %s | %s", message, e)
            reply = self.on_message_error(message, e)

        return reply

    def on_message(self, message):
        name = message.get("method", None)
        if name:
            method = self.methods.get(name, None)
            if method:
                return method(message)
            else:
                raise KeyError("no such method: %s" % name)
        else:
            raise KeyError("key: 'method' not in query")

    def on_message_error(self, message, e):
        message['error'] = {"error": -1, "message": str(e)}
        message["result"] = {}
        return message


def no_error(dct):
    dct["error"] = {"error": 0}


def heartbeat(dct):
    res = dct.copy()
    res.pop("params")
    logging.debug("heartbeat | %s" % res)
    now_timestamp = time.time()
    res["result"] = {"time": now_timestamp, "sub_hash": ""}
    no_error(res)
    res['time'] = now_timestamp * 1000
    return res


def login(dct):
    res = dct.copy()
    params = res.pop("params")
    res["result"] = "username: %s" % params["username"]
    no_error(res)
    res['time'] = datetime.now().timestamp()
    logging.warning("login | %s" % res)
    return res


def logout(dct):
    res = dct.copy()
    res["result"] = True
    no_error(res)
    res['time'] = datetime.now().timestamp()
    logging.warning("logout | %s" % res)
    return res


class RegularReplier(Replier):

    no_error = no_error

    def __init__(self):
        super().__init__()
        self.methods[".sys.heartbeat"] = heartbeat
        self.methods["auth.login"] = login
        self.methods["auth.logout"] = logout