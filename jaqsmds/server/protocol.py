# encoding:utf-8
from queue import Queue, Empty
from datetime import datetime, timedelta
from threading import RLock, Thread
from time import sleep
import logging


ENCODE = "utf-8"
READY = b"ready"
IS_READY = b"isReady"


class _Thread(Thread):

    def __init__(self, *args, **kwargs):
        super(_Thread, self).__init__(*args, **kwargs)
        self._running = False

    def start(self):
        self._running = True
        super(_Thread, self).start()

    def run(self):
        while self._running:
            self.routing()

    def routing(self):
        pass


class FrontendReceiver(_Thread):

    def __init__(self, mq=None):
        super(FrontendReceiver, self).__init__()
        self.mq = mq if isinstance(mq, Queue) else Queue

    def receive(self, client, msg):
        self.mq.put((client, msg))


class FrontendSender(_Thread):

    def __init__(self, mq=None):
        super(FrontendSender, self).__init__()
        self.mq = mq if isinstance(mq, Queue) else Queue

    def routing(self):
        try:
            msg = self.mq.get(timeout=1)
        except Empty:
            pass
        else:
            self.send(*msg)

    def send(self, client, msg):
        pass


class BackendSender(_Thread):

    def __init__(self, pool, mq=None):
        super(BackendSender, self).__init__()
        self.pool = pool
        self.mq = mq if isinstance(mq, Queue) else Queue

    def routing(self):
        try:
            msg = self.mq.get(timeout=1)
        except Empty:
            pass
        else:
            if msg[1] == IS_READY:
                self.send(*msg)
            else:
                try:
                    worker = self.pool.popitem()
                except KeyError:
                    self.mq.put(msg)
                else:
                    self.send(worker, *msg)

    def send(self, *msg):
        pass


class BackendReceiver(_Thread):

    def __init__(self, pool, mq=None):
        super(BackendReceiver, self).__init__()
        self.pool = pool
        self.mq = mq if isinstance(mq, Queue) else Queue

    def receive(self, worker, *msg):
        if msg[0] != READY:
            self.mq.put(msg)
        self.pool.put(worker)


def lock_wrapper(lock):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            lock.acquire()
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                lock.release()
                raise e
            else:
                lock.release()
            return result
        return wrapped

    return wrapper


class WorkerPool(object):

    lock = RLock()
    wrapper = lock_wrapper(lock)

    def __init__(self):
        self.workers = {}

    @wrapper
    def put(self, value):
        self.workers[value] = datetime.now()

    @wrapper
    def __getitem__(self, item):
        return self.workers[item]

    @wrapper
    def pop(self, item):
        return self.workers.pop(item)

    @wrapper
    def popitem(self):
        return self.workers.popitem()[0]

    @wrapper
    def timeout(self, limit):
        return list(self._timeout(limit))

    def _timeout(self, limit):
        now = datetime.now()
        for worker, time in self.workers.copy().items():
            if now - time > limit:
                self.workers.pop(worker)
                yield worker


class WorkerScanner(_Thread):

    def __init__(self, pool, mq, timeout=10):
        super(WorkerScanner, self).__init__()
        self.pool = pool
        self.timeout = timedelta(seconds=timeout) if isinstance(timeout, int) else timeout
        self.mq = mq
        self._running = False

    def routing(self):
        workers = self.pool.timeout(self.timeout)
        for worker in workers:
            logging.debug("%s timeout" % worker)
            self.mq.put((worker, IS_READY))
        sleep(1)


class Worker(object):

    def receive(self, *msg):
        if msg[0] == IS_READY:
            self.ready()
        else:
            self.reply(msg)

    def ready(self):
        self.send(READY)
        logging.debug("ready")

    def reply(self, msg):
        pass

    def send(self, *msg):
        pass
