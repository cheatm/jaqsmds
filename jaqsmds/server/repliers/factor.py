from jaqsmds.server.repliers.utils import Jset3DReader, fill_field_filter, date2str, time_range_daily


class FactorReader(Jset3DReader):

    def __init__(self, db):
        super(FactorReader, self).__init__(db, view="factor")

    @staticmethod
    def field_filter(string):
        return fill_field_filter(string, 'datetime')

    def split_range(self, dct):
        r = time_range_daily(dct.pop("start", None), dct.pop("end", None))
        if len(r):
            yield "datetime", r

    def _read(self, symbol, f, p):
        data = super(FactorReader, self)._read(symbol, f, p)
        data["symbol"] = symbol
        data["datetime"] = data["datetime"].apply(date2str)
        return data
