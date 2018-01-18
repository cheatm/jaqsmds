from jaqsmds.server.worker import run_worker
from jaqsmds.server.proxy import run_proxy
import multiprocessing
from time import sleep
from jaqsmds import logger
import logging
from datetime import datetime


# 管理进程，保存了服务和子进程运行所需的全部环境变量
class ServerManager(object):

    def __init__(self, frontend, backend, mongodb_url, process_count=5,
                 log_dir="", level=logging.WARNING, db_map=None):
        super(ServerManager, self).__init__()
        self.frontend = frontend
        self.backend = backend
        self.mongodb_url = mongodb_url
        self.process_count = process_count
        self.log_dir = log_dir
        self.level = level
        self.db_map = db_map
        self.proxy = None
        self.workers = {}
        self._running = False
        self._last_check_time = datetime.now()

    # 启动一个新的路由进程
    def new_proxy(self):
        logging.warning("Starting router.")
        proxy = multiprocessing.Process(target=run_proxy,
                                        args=(self.frontend, self.backend))
        proxy.daemon = True
        proxy.start()
        logging.warning("Router working.")
        return proxy

    # 启动一个新的工作进程
    def new_worker(self, name):
        logging.warning("Starting Worker-%s." % name)
        p = multiprocessing.Process(target=run_worker,
                                    args=(self.backend, self.mongodb_url,
                                          self.log_dir, "Worker-%s.log" % name,
                                          self.level, self.db_map))
        p.daemon = True
        p.start()
        logging.warning("Worker-%s working." % name)
        return p

    # 初始化，新建并启动路由进程和工作进程
    def initialize(self):
        self.proxy = self.new_proxy()
        for i in range(self.process_count):
            self.workers[i] = self.new_worker(i)

    # 服务启动
    def start(self):
        self._running = True
        self.initialize()
        self.run()

    # 每秒检查子进程状态，进程关闭则重启
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
            logging.error("Router process down")
            del self.proxy
            self.proxy = self.new_proxy()

    def check_workers(self):
        for name, worker in self.workers.copy().items():
            if not worker.is_alive():
                logging.error("Worker-%s down", name)
                del self.workers[name]
                self.workers[name] = self.new_worker(name)


def start_service(frontend, backend, mongodb_url, process=5, log_dir=None,
                  level=logging.WARNING, db_map=None):

    logger.init(log_dir, "main.log", level)

    log_variables(frontend=frontend, backend=backend, mongodb_url=mongodb_url,
                  process=process, log_dir=log_dir, level=level, **db_map)

    manager = ServerManager(frontend, backend, mongodb_url, process, log_dir, level, db_map)
    manager.start()


# 输出环境信息
def log_variables(**kwargs):
    logging.warning("Init Service")
    for name, value in kwargs.items():
        logging.warning("%s: %s", name, value)

