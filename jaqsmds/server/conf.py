import os
import logging


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
FRONTEND = os.environ.get('FRONTEND', 'tcp://0.0.0.0:23000')
BACKEND = os.environ.get('BACKEND', 'tcp://127.0.0.1:23001')
PROCESS = int(os.environ.get('PROCESS', 5))
LOG_DIR = os.environ.get('LOG_DIR', None)
LEVEL = int(os.environ.get('LEVEL', logging.WARNING))
MONGODB_URI = os.environ.get("MONGODB_URI", "localhost")
AUTH = os.environ.get("AUTH", "users.auth")
AUTH_EXPIRE = int(os.environ.get("AUTH_EXPIRE", 300))


def variables():
    return {name: str(globals()[name]) for name in
            ["MONGODB_URI", "REDIS_URL", "FRONTEND", "BACKEND", "PROCESS",
             "LOG_DIR", "LEVEL", "AUTH", "AUTH_EXPIRE"]}

