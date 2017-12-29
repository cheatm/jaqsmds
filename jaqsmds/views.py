from itertools import chain
import six


def iter_filter(string):
    for pair in string.replace(" ", "").split("&"):
        key, value = pair.split("=")
        yield key, value


class JsetView(object):

    view = ""
    compulsory = ()
    optional = ()
    fields = ()
    data_format = "pandas"
    defaults = {}

    def __init__(self, db=None):
        self.db = db if isinstance(db, six.string_types) else self.view

    def query(self, *fields, **filters):
        self._check(filters)
        if len(fields):
            f = ",".join(fields)
        else:
            f = ",".join(self.fields)
        return {
            "view": self.view,
            "filter": self.query_filter(**filters),
            "fields": f,
            "data_format": self.data_format
        }

    def _check(self, arguments):
        for key in self.compulsory:
            if key not in arguments:
                try:
                    default = self.defaults[key]
                except KeyError:
                    raise ValueError("'%s' is compulsory but not in arguments or defaults." % key)
                else:
                    arguments[key] = default

    @staticmethod
    def query_filter(**kwargs):
        return "&".join(["{}={}".format(*item) for item in kwargs.items()])

    @property
    def arguments(self):
        return list(chain(self.compulsory, self.optional))

    def check_filter(self, dct):
        for key in self.compulsory:
            value = dct.get(key, None)
            if value is None:
                raise ValueError("%s is Compulsory" % key)

    def mongo_filter(self, string):
        _filter = dict(iter_filter(string))
        self.check_filter(_filter)
        for key, value in list(_filter.items()):
            if ',' in value:
                del _filter[key]
                _filter.setdefault("$or", []).append({key: value})

        return _filter

    def read_mongo(self, client, _filter, projection):
        _filter = self.mongo_filter(_filter)
        projection = {key: 1 for key in projection.replace(" ", "").split(",")}

    def _read_mongo(self, client, _filter, projection):
        col, db = self.db.split('.')
        collection = client[db][col]

