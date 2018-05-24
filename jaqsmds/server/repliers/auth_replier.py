from jaqsmds.server.repliers.handlers import JsetHandler
from jaqsmds.server.repliers.basic import Replier
from jaqsmds.server import auth
import logging


class AuthReplier(Replier):

    def __init__(self):
        super(AuthReplier, self).__init__()
        self.methods[".sys.heartbeat"] = auth.heartbeat
        self.methods["auth.login"] = auth.login
        self.methods["auth.logout"] = auth.logout
        self.jset = AuthJsetHandler()
        self.methods["jset.query"] = self.jset.handle

    def handle(self, request):
        try:
            reply = self.on_message(request)
        except Exception as e:
            logging.error("message | %s | %s", request, e)
            reply = self.on_message_error(request, e)
        return reply


class AuthJsetHandler(JsetHandler):

    def handle(self, dct):
        # check permission by client
        client = dct["client"]
        permission = auth.permission(dct)
        # has permission
        if permission is not None:
            # remove unauthorized fields
            fields, our_range = self.check_fields(dct["params"]["fields"], permission)
            # reply authorized fields
            if fields is not None:
                dct["params"]["fields"] = fields
                reply = super(AuthJsetHandler, self).handle(dct)
                if reply["error"]["error"] == 0:
                    reply["error"]["message"] = "Fields out of restrict: %s" % our_range
                return reply
            # all fields are unauthorized
            else:
                logging.warning("Message | %s | %s | All fields out of restrict", client, dct)
                dct["error"] = {"error": -1, "message": "Fields out of restrict: %s" % our_range}
                return dct
        # no permission
        else:
            logging.warning("Message | %s | %s | not login", client, dct)
            dct["error"] = {"error": -1000, "message": "not login"}
            return dct

    @staticmethod
    def check_fields(fields, restricted):
        res = set(map(lambda b: b.decode(), restricted))
        origin = set(fields.split(","))
        if len(origin) == 0:
            return ",".join(res), ",".join(res)

        out_range = origin.difference(res)
        permitted = origin.difference(out_range)
        if len(permitted) == 0:
            return None, ",".join(out_range)

        return ",".join(permitted), ",".join(out_range)
