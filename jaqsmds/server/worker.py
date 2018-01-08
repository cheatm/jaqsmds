from datetime import datetime
import logging
import zmq
from jaqsmds.server.protocol import Worker, ENCODE
from jaqsmds.utils.zmqs import del_blank, fill_blank

#
# class ZMQWorker(Worker):
#
#     def __init__(self, backend, replier):
#         self.backend = backend
#         self.identity = "Worker-{}".format(datetime.now().timestamp())
#         self.identity_byte = self.identity.encode(ENCODE)
#         self.socket = zmq.Context().socket(zmq.REQ)
#         self.socket.identity = self.identity_byte
#         self.socket.connect(self.backend)
#         self.replier = replier
#
#     def run(self):
#         self.ready()
#         while True:
#             try:
#                 message = self.socket.recv_multipart()
#             except zmq.ZMQError as e:
#                 logging.error("%s: %s", self.identity, e)
#             else:
#                 msg = del_blank(message)
#                 self.receive(*msg)
#
#     def reply(self, msg):
#         client = msg[0]
#         result = self.replier.handle(msg[1])
#         self.send(client, result)
#
#     def send(self, *msg):
#         if len(msg) == 1:
#             self.socket.send(msg[0])
#         else:
#             message = fill_blank(msg)
#             self.socket.send_multipart(message)


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
                    client, msg = message
                    result = self.replier.handle(msg)
                    self.socket.send_multipart([client, result])
            except Exception as e:
                logging.error(e)
                break


def run_worker(backend, mongodb_url, log_file=None, level=logging.WARNING):
    from jaqsmds.server.repliers.db_replier import DBReplier
    from logging.handlers import RotatingFileHandler
    from logging import StreamHandler

    if log_file:
        logging.basicConfig(
            format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
            handlers=[RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=5),
                      StreamHandler()],
            level=level
        )
    else:
        logging.basicConfig(
            format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
            handlers=[StreamHandler()],
            level=level
        )

    worker = SimpleWorker(backend, DBReplier(mongodb_url))
    worker.run()
