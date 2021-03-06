from jaqsmds import logger
import logging
import zmq


def run_proxy(front, back):
    logger.init(None)

    context = zmq.Context()

    # Socket facing clients
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(front)
    logging.warning("Frontend binds to %s.", front)

    # Socket facing services
    backend = context.socket(zmq.DEALER)
    backend.bind(back)
    logging.warning("Backend binds to %s.", back)

    result = zmq.proxy(frontend, backend)
    logging.warning("Proxy returns: %s.", result)

    frontend.close()
    backend.close()
    context.term()
    logging.warning("Proxy terminated.")


def run_worker(backend):
    socket = zmq.Context().socket(zmq.DEALER)
    identity_byte = b"worker"
    socket.identity = identity_byte
    socket.connect(backend)
    message = socket.recv_multipart()
    client = message[0]
    socket.send_multipart([client, b'close'])


def run_client(frontend):
    socket = zmq.Context().socket(zmq.DEALER)
    identity_byte = b"client"
    socket.identity = identity_byte
    socket.connect(frontend)
    socket.send_multipart([b"high"])
    result = socket.recv_multipart()
    print(result)
