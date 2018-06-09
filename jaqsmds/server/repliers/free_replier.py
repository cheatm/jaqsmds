from jaqsmds.server.repliers.basic import RegularReplier
from jaqsmds.server.repliers.handlers import JsetHandler, JsdHandler, JsiHandler
from queue import Queue, Empty
from threading import Thread


class FreeReplier(RegularReplier):

    def __init__(self):
        super(FreeReplier, self).__init__()
        self.jset = JsetHandler()
        self.jsd = JsdHandler()
        self.jsi = JsiHandler()
        self.methods["jset.query"] = self.jset.handle
        self.methods["jsd.query"] = self.jsd.handle
        self.methods["jsi.query"] = self.jsi.handle
        self.input = Queue()
        self.output = Queue()
        self._running = False
        self.thread = Thread(target=self.run)

    def run(self):
        while self._running or self.input.qsize():
            try:
                client, message = self.input.get(timeout=2)
            except Empty:
                continue
            
            result = self.handle(message)
            self.output.put([client, result])

    def start(self):
        self._running = True
        self.thread.start()

    def stop(self):
        self._running = False
        self.thread.join()        

    @property
    def unfinished(self):
        return self.input.qsize() + self.output.qsize()

    def put(self, client, message):
        if message.get("method", None) == ".sys.heartbeat":
            self.output.put([client, self.methods[".sys.heartbeat"](message)])
        else:
            self.input.put([client, message])
    
    def ready(self):
        while True:
            try:
                yield self.output.get(timeout=0.1)
            except Empty:
                break