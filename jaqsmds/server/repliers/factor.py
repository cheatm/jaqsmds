from jaqsmds.server.repliers.db_replier import MongodbHandler, read


def time_range(start=None, end=None):
    dct = {}
    if start:
        dct['$gte'] = start
    if end:
        dct["%lte"] = end
    return dct


class FactorHandler(MongodbHandler):

    def __init__(self, client):
        super(FactorHandler, self).__init__(client)
        self.db = client['factor']

    def _read(self, symbol, _filter, projection):
        collection = self.db[symbol]
        data = read(collection, _filter, projection)
        data["symbol"] = symbol
        return data