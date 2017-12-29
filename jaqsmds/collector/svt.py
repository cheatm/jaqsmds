from pymongo import MongoClient
import logging


def translate(source, target, index=()):
    data = list(source.find(projection={"_id": 0}))
    result = target.insert_many(data)
    for idx in index:
        target.create_index(idx, background=True)
    return result


def translate_db(source, target, index=()):
    for name in source.collection_names():
        try:
            result = translate(source[name], target[name], index)
        except Exception as e:
            logging.error("%s | %s", name, e)
        else:
            logging.warning("%s | %s", name, "OK")


if __name__ == '__main__':
    local = MongoClient(port=37017)
    remote = MongoClient("192.168.0.102")
    db_name = "SecDailyIndicator"

    translate_db(local[db_name], remote[db_name],
                 ("symbol", 'ann_date', 'comp_type_code', 'act_ann_date', 'report_date', 'report_type', "update_flag"))
