import logging


db_map = {
    "factor": "factor"
}


server_config = {
    "frontend": "tcp://0.0.0.0:23000",
    "backend": "tcp://127.0.0.1:23001",
    "mongodb_url": "mongodb://localhost:27017",
    "process": 5,
    "log_dir": "",
    "level": logging.WARNING,
    "timeout": 10
}


def init(dct):
    for name, configs in dct.items():
        conf = globals()[name]
        for key, value in configs:
            conf[key] = value


def init_file(path):
    import json
    import os

    if os.path.isfile(path):
        conf = json.load(open(path))
        init(conf)