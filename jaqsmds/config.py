import logging


server_config = {
    "frontend": "tcp://0.0.0.0:23000",
    "backend": "tcp://127.0.0.1:23001",
    "mongodb_url": "mongodb://localhost:27017",
    "process": 5,
    "log_dir": "",
    "level": logging.WARNING,
    "timeout": 10
}