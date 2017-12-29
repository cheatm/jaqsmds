from jaqsmds.server.worker import run_worker
from jaqsmds.server.router import run_router
import multiprocessing
from time import sleep
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import logging
import os


def start_service(frontend, backend, mongodb_url, process=5, log_dir="", level=logging.WARNING, timeout=10):
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
        handlers=[RotatingFileHandler(os.path.join(log_dir, "router.log"), maxBytes=1024*1024, backupCount=5),
                  StreamHandler()],
        level=level
    )

    init_log(frontend=frontend, backend=backend, mongodb_url=mongodb_url, process=process, log_dir=log_dir,
             level=level, timeout=timeout)

    run_router(frontend, backend)
    logging.warning("Start router")

    for i in range(process):
        p = multiprocessing.Process(target=run_worker,
                                    args=(backend, mongodb_url, os.path.join(log_dir, "Worker-%s.log" % i), level))
        p.daemon = True
        p.start()
        logging.warning("Start Worker-%s" % i)

    while True:
        sleep(1)


def init_log(**kwargs):
    logging.warning("Init Service")
    for name, value in kwargs.items():
        logging.warning("%s: %s", name ,value)


if __name__ == '__main__':
    start_service("tcp://0.0.0.0:23000", "tcp://127.0.0.1:23001", "mongodb://localhost:37017",
                  log_dir="D:/jaqsmds/logs")