from jaqsmds.server.worker import run_worker
from jaqsmds.server.proxy import run_proxy
import multiprocessing
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from time import sleep
import logging
import os


class ServerManager(object):

    def __init__(self, frontend, backend, mongodb_url, process_count=5, log_dir="", level=logging.WARNING, timeout=10):
        super(ServerManager, self).__init__()
        self.frontend = frontend
        self.backend = backend
        self.mongodb_url = mongodb_url
        self.process_count = process_count
        self.log_dir = log_dir
        self.level = level
        self.timeout = timeout
        self.proxy = None
        self.workers = {}
        self._running = False

    def new_proxy(self):
        logging.warning("Starting router.")
        proxy = multiprocessing.Process(target=run_proxy,
                                        args=(self.frontend, self.backend))
        proxy.daemon = True
        proxy.start()
        logging.warning("Router working.")
        return proxy

    def new_worker(self, name):
        logging.warning("Starting Worker-%s." % name)
        p = multiprocessing.Process(target=run_worker,
                                    args=(self.backend, self.mongodb_url,
                                          os.path.join(self.log_dir, "Worker-%s.log" % name), self.level))
        p.daemon = True
        p.start()
        logging.warning("Worker-%s working." % name)
        return p

    def initialize(self):
        self.proxy = self.new_proxy()
        for i in range(self.process_count):
            self.workers[i] = self.new_worker(i)

    def start(self):
        self._running = True
        self.initialize()
        self.run()
        # super(ServerManager, self).start()

    def run(self):
        while self._running:
            try:
                self.check_proxy()
                self.check_workers()
                sleep(1)
            except KeyboardInterrupt:
                import sys
                sys.exit()

    def check_proxy(self):
        if not self.proxy.is_alive():
            del self.proxy
            self.proxy = self.new_proxy()

    def check_workers(self):
        for name, worker in self.workers.copy().items():
            if not worker.is_alive():
                self.workers.pop(name)
                self.workers[name] = self.new_worker(name)


def start_service(frontend, backend, mongodb_url, process=5, log_dir="", level=logging.WARNING, timeout=10):
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
        handlers=[RotatingFileHandler(os.path.join(log_dir, "main.log"), maxBytes=1024*1024, backupCount=5),
                  StreamHandler()],
        level=level
    )

    init_log(frontend=frontend, backend=backend, mongodb_url=mongodb_url, process=process, log_dir=log_dir,
             level=level, timeout=timeout)

    manager = ServerManager(frontend, backend, mongodb_url, process, log_dir, level, timeout)
    manager.start()


def init_log(**kwargs):
    logging.warning("Init Service")
    for name, value in kwargs.items():
        logging.warning("%s: %s", name, value)

