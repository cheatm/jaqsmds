from jaqsmds.server.worker import SimpleWorker
from jaqs.data.dataapi.jrpc_py import _unpack_msgpack_snappy, _pack_msgpack_snappy


class AuthWorker(SimpleWorker):

    def reply(self, message):
        client, msg = message
        msg = _unpack_msgpack_snappy(msg)
        msg["client"] = client
        result = self.replier.handle(msg)
        reply = _pack_msgpack_snappy(result)
        self.socket.send_multipart([client, reply])


def run_worker(name):
    from datautils.fxdayu import instance
    from jaqsmds.server import auth
    from jaqsmds.server import conf
    from jaqsmds.server.repliers.auth_replier import AuthReplier
    from jaqsmds import logger

    logger.init(conf.LOG_DIR, name, conf.LEVEL)
    instance.init()
    auth.init()

    worker = AuthWorker(conf.BACKEND, AuthReplier())
    worker.run()
