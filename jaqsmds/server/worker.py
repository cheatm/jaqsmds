from datetime import datetime
from jaqs.data.dataapi.jrpc_py import _unpack_msgpack_snappy, _pack_msgpack_snappy
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
        while True:
            try:
                message = self.socket.recv_multipart()
                if len(message) == 2:
                    self.reply(message)
            except Exception as e:
                logging.error(e)
                break

    def reply(self, message):
        client, msg = message
        msg = _unpack_msgpack_snappy(msg)
        result = self.replier.handle(msg)
        reply = _pack_msgpack_snappy(result)
        self.socket.send_multipart([client, reply])


# 启动工作进程
def run_worker(name):
    from datautils.fxdayu import instance
    from jaqsmds.server import conf
    from jaqsmds.server.repliers.free_replier import FreeReplier
    from jaqsmds import logger

    logger.init(conf.LOG_DIR, name, conf.LEVEL)
    instance.init()
    worker = SimpleWorker(conf.BACKEND, FreeReplier())
    worker.run()