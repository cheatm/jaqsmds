from jaqsmds.server.protocol import BackendReceiver, BackendSender, FrontendReceiver, FrontendSender, WorkerScanner, \
    WorkerPool
from jaqsmds.utils.zmqs import fill_blank
import logging
import zmq


class ZMQBackendReceiver(BackendReceiver):

    def __init__(self, socket, pool, mq=None):
        super(ZMQBackendReceiver, self).__init__(pool, mq)
        self.socket = socket

    def routing(self):
        try:
            message = self.socket.recv_multipart()
        except zmq.ZMQError as e:
            logging.error("ZMQBackendReceiver: %s", e)
        else:
            self.receive(*message[0:None:2])


class ZMQBackendSender(BackendSender):

    def __init__(self, socket, pool, mq=None):
        super(ZMQBackendSender, self).__init__(pool, mq)
        self.socket = socket

    def send(self, *msg):
        message = fill_blank(msg)
        self.socket.send_multipart(message)


class ZMQFrontendSender(FrontendSender):

    def __init__(self, socket, mq=None):
        super(ZMQFrontendSender, self).__init__(mq)
        self.socket = socket

    def send(self, client, msg):
        self.socket.send_multipart([client, msg])


class ZMQFrontendReceiver(FrontendReceiver):

    def __init__(self, socket, mq=None):
        super(ZMQFrontendReceiver, self).__init__(mq)
        self.socket = socket

    def routing(self):
        try:
            request = self.socket.recv_multipart()
        except zmq.ZMQError as e:
            logging.error("ZMQFrontendReceiver", e)
        else:
            if len(request) == 2:
                self.receive(*request)
            else:
                logging.error("ZMQFrontendReceiver received: %s, not supported", request)


from queue import Queue


def main():
    FRONT = "tcp://0.0.0.0:23000"
    BACK = "tcp://127.0.0.1:23001"

    context = zmq.Context()
    front2back = Queue()
    back2front = Queue()
    pool = WorkerPool()

    backend = context.socket(zmq.ROUTER)
    backend.bind(BACK)
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(FRONT)

    fs = ZMQFrontendSender(frontend, back2front)
    fr = ZMQFrontendReceiver(frontend, front2back)
    bs = ZMQBackendSender(backend, pool, front2back)
    br = ZMQBackendReceiver(backend, pool, back2front)
    sc = WorkerScanner(pool, front2back)
    fs.setDaemon(True)

    for t in [sc, fs, fr, bs, br]:
        t.setDaemon(True)
        t.start()


def run_router(FRONTEND, BACKEND, timeout=10):
    context = zmq.Context()
    front2back = Queue()
    back2front = Queue()
    pool = WorkerPool()

    backend = context.socket(zmq.ROUTER)
    backend.bind(BACKEND)
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(FRONTEND)

    fs = ZMQFrontendSender(frontend, back2front)
    fr = ZMQFrontendReceiver(frontend, front2back)
    bs = ZMQBackendSender(backend, pool, front2back)
    br = ZMQBackendReceiver(backend, pool, back2front)
    sc = WorkerScanner(pool, front2back, timeout)
    fs.setDaemon(True)

    for t in [sc, fs, fr, bs, br]:
        t.setDaemon(True)
        t.start()


if __name__ == '__main__':
    main()