from datetime import datetime
from jaqs.data.dataapi.jrpc_py import _unpack_msgpack_snappy, _pack_msgpack_snappy
from threading import Thread
import logging
import zmq


ENCODE = "utf-8"


# 工作进程，接收客户端请求读取数据返回
class SimpleWorker():

    def __init__(self, backend, replier):
        self.backend = backend
        self.identity = "Worker-{}".format(datetime.now().timestamp())
        self.identity_byte = self.identity.encode(ENCODE)
        self.socket = zmq.Context().socket(zmq.DEALER)
        self.socket.identity = self.identity_byte
        self.socket.connect(self.backend)
        self.replier = replier

    def run(self):
        self.replier.start()
        while True:
            try:
                message = self.socket.recv_multipart(zmq.NOBLOCK)
                if len(message) == 2:
                    self.reply(message)
            except zmq.error.Again:
                pass
            except Exception as e:
                logging.error(e)
                self.wait_and_reply()
                break
            finally:
                self.fetch_and_reply()

    def wait_and_reply(self):
        while self.replier.unfinished:
            self.fetch_and_reply()

    def fetch_and_reply(self):
        for client, result in self.replier.ready():
            try:
                reply = _pack_msgpack_snappy(result)
                self.socket.send_multipart([client, reply])
            except Exception as e:
                logging.error("reply send_multipart | %s | %s", client, e)

    def reply(self, message):
        client, msg = message
        msg = _unpack_msgpack_snappy(msg)
        self.replier.put(client, msg)
        # result = self.replier.handle(msg)
        # reply = _pack_msgpack_snappy(result)
        # self.socket.send_multipart([client, reply])


# 启动工作进程
def run_worker(name):
    from datautils.fxdayu import instance
    from jaqsmds.server import conf
    from jaqsmds.server.repliers.free_replier import FreeReplier
    from jaqsmds import logger
    import json
    import os

    logger.init(conf.LOG_DIR, name, conf.LEVEL)
    if os.path.isfile(conf.FILE):
        with open(conf.FILE) as f:
            config = json.load(f)
            if isinstance(config, dict):
                instance.init(config)
            elif isinstance(config, list):
                instance.init(config)
    else:
        instance.init_mongodb_example()

    worker = SimpleWorker(conf.BACKEND, FreeReplier())
    worker.run()


def main():
    run_worker("test")

if __name__ == '__main__':
    main()