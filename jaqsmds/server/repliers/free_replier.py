from jaqsmds.server.repliers.basic import RegularReplier
from jaqsmds.server.repliers.handlers import JsetHandler, JsdHandler, JsiHandler


class FreeReplier(RegularReplier):

    def __init__(self):
        super(FreeReplier, self).__init__()
        self.jset = JsetHandler()
        self.jsd = JsdHandler()
        self.jsi = JsiHandler()
        self.methods["jset.query"] = self.jset.handle
        self.methods["jsd.query"] = self.jsd.handle
        self.methods["jsi.query"] = self.jsi.handle
