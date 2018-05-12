from jaqsmds.server import conf
from datautils.fxdayu import conf as data_conf
from jaqsmds.server.proxy import run_proxy
from jaqsmds.server.auth_worker import run_worker
import multiprocessing
from time import sleep
from jaqsmds import logger
import logging
from datetime import datetime


# 进程管理
class ServerManager(object):

    def __init__(self):
        self.proxy = None
        self.workers = {}
        self._running = False
        self._last_check_time = datetime.now()
        self.variables = {}
        self.variables.update(conf.variables())
        self.variables.update(data_conf.variables())

    # 启动一个新的路由进程
    def new_proxy(self):
        logging.warning("Starting router.")
        proxy = multiprocessing.Process(target=run_proxy,
                                        name="Router",
                                        args=(conf.FRONTEND, conf.BACKEND))
        proxy.daemon = True
        proxy.start()
        logging.warning("Router working.")
        return proxy

    # 初始化，新建并启动路由进程和工作进程
    def initialize(self):
        self.proxy = self.new_proxy()
        for i in range(conf.PROCESS):
            self.workers[i] = self.new_worker(i)

    # 启动一个新的工作进程
    def new_worker(self, name):
        worker_name = "Worker-%s" % name
        logging.warning("Starting %s." % worker_name)

        p = multiprocessing.Process(target=run_worker,
                                    name=worker_name,
                                    args=(worker_name,))
        p.daemon = True
        p.start()
        logging.warning("Worker-%s working." % name)
        return p

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

    # 检查路由进程
    def check_proxy(self):
        if not self.proxy.is_alive():
            logging.error("Router process down")
            del self.proxy
            self.proxy = self.new_proxy()

    # 检查工作进程
    def check_workers(self):
        for name, worker in self.workers.copy().items():
            if not worker.is_alive():
                logging.error("Worker-%s down", name)
                del self.workers[name]
                self.workers[name] = self.new_worker(name)


def log_variables(**kwargs):
    for name, value in kwargs.items():
        logging.warning("%s: %s", name, value)


def start_service(**variables):
    from importlib import reload
    import os

    # 更新环境变量并reload配置模块
    os.environ.update(variables)
    reload(conf)
    reload(data_conf)

    # 初始化日志
    logger.init(conf.LOG_DIR, "main.log", conf.LEVEL)
    logging.warning("Init Service")

    # 输出配置变量
    cv = conf.variables()
    log_variables(**cv)
    log_variables(**{name: variables[name] for name in set(variables).difference(set(cv))})

    # 启动进程管理
    manager = ServerManager()
    manager.start()
