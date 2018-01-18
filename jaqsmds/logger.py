import logging
import os


def init(log_dir, name=None, level=None):
    if log_dir:
        from logging.handlers import RotatingFileHandler
        handlers = [RotatingFileHandler(os.path.join(log_dir, name), maxBytes=1024*1024, backupCount=5),
                    logging.StreamHandler()]
    else:
        handlers = [logging.StreamHandler()]
    
    logging.basicConfig(
        format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s",
        handlers=handlers,
        level=level
    )