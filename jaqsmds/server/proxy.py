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