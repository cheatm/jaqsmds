from redis import StrictRedis

sr = StrictRedis.from_url("redis://192.168.0.102:6379/1")
print(sr.set("test", "1"))