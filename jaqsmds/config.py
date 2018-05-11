import logging
import os

JSET = "jset.query"
JSD = "jsd.query"
JSI = "jsi.query"


db_map = {
    JSET: {"lb": "lb",
           "jz": "jz",
           "factor": "factors",
           "lb.secDailyIndicator": "SecDailyIndicator",
           "lb.mfNav": "MFNav",
           "lb.mfDividend": "MFDividend",
           "lb.mfPortfolio": "MFPortfolio",
           "lb.mfBondPortfolio": "MFBondPortfolio"},
    JSD: "Stock_D",
    JSI: {"stock": "Stock_1M", "future": "future_1M"}
}


server_config = {
    'frontend': os.environ.get('FRONTEND', 'tcp://0.0.0.0:23000'),
    'backend': os.environ.get('BACKEND', 'tcp://127.0.0.1:23001'),
    'mongodb_url': os.environ.get('MONGODB_URL', 'mongodb://localhost:27017'),
    'process': int(os.environ.get('PROCESS', 5)),
    'log_dir': os.environ.get('LOG_DIR', None),
    "level": os.environ.get('LEVEL', logging.WARNING)
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

