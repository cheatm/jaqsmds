from jaqs_fxdayu.research.signaldigger import process
from jaqs_fxdayu.data import DataApi
import os


factors = ['E010030D', 'E010045D', 'D040005D', 'E010083D', 'L010082D', 'A010005D', 'B010012A']


class SubscribeFactor(object):

    def __init__(self, root):
        self.root = root
        self.api = DataApi("tcp://120.78.130.50:8910")
        self.api.login("guojin", "guojin")

    def load(self, start, end, field):
        data, msg = self.api.query("fxdayu.factor", "start_date=%s&end_date=%s" % (start, end), field)
        return data.pivot("trade_date", "symbol", field)

    def filename(self, _dir, field, start, end, tag="origin"):
        return os.path.join(self.root, _dir, "{}#{}#{}_{}.csv".format(field, tag, start, end))

    def download(self, start, end):
        for field in factors:
            origin = self.load(start, end, field)
            standardized = process.standardize(process.mad)





if __name__ == '__main__':
    print(load(api, 20170101, 20180525, "E010030D"))
