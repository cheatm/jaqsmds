import logging


db_map = {
    "jset.query": {"lb": "lb",
                   "jz": "jz",
                   "factor": "factor",
                   "lb.secDailyIndicator": "SecDailyIndicator"},
    "jsd.query": "Stock_D"
}


server_config = {
    "frontend": "tcp://0.0.0.0:23000",
    "backend": "tcp://127.0.0.1:23001",
    "mongodb_url": "mongodb://localhost:27017",
    "process": 5,
    "log_dir": "",
    "level": logging.WARNING,
}


conf = {"server_config": server_config,
        "db_map": db_map}


def init(dct):
    update(conf, dct)


def update(origin, new):
    for key, value in origin.items():
        new_value = new.get(key, None)
        if new_value:
            if isinstance(value, dict):
                update(value, new_value)
            else:
                origin[key] = new_value


def init_file(path):
    import json
    import os

    if os.path.isfile(path):
        conf = json.load(open(path))
        init(conf)
