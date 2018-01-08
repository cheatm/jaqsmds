from jaqs.data import DataApi
from jaqsmds.collector.structure import *
from datetime import datetime
from time import sleep


# start = datetime.now()
api = DataApi("tcp://127.0.0.1:23000")
# api = DataApi()
api.login("13823156147", "eyJhbGciOiJIUzI1NiJ9.eyJjcmVhdGVfdGltZSI6IjE1MTI3ODY3ODYxODMiLCJpc3MiOiJhdXRoMCIsImlkIjoiMTM4MjMxNTYxNDcifQ.Lt4orfuPoP5xVM_t3n4SdC7xwPNDoloHdvCAWU4JfYQ")
# data, msg = api.daily("000001.SZ", 20170101, 20180101,)
# data = api.query(**InstrumentInfo(status=1, market="SZ,SH", inst_type="1"))
data, msg = api.query("factor", "symbol=000001.XSHE,000002.XSHE&start=2017-01-01", "PB,PE,ACCA,ARTRate", "pandas")
print(data)

# symbols = ",".join(data["symbol"])
#
# data, msg = api.query(**SecIndustry(symbol=symbols))
# print(data, msg, sep="\n")
# end = datetime.now()
# print(end-start)

# data, msg = api.query(view="lv.x")
# print(msg)