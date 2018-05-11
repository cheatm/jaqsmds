from datetime import datetime
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
        result = self.replier.handle(msg)
        self.socket.send_multipart([client, result])


# 启动工作进程
def run_worker(backend, mongodb_url, log_dir=None, log_file=None, level=logging.WARNING, db_map=None):
    from jaqsmds.server.repliers.db_replier import DBReplier
    from jaqsmds import logger

    logger.init(log_dir, log_file, level)
    worker = SimpleWorker(backend, DBReplier(mongodb_url, db_map))
    worker.run()
