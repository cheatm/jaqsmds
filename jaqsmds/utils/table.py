import pandas as pd

TABLE = """symbol	string	证券代码
prt_enddate	string	截止日期
currency	string	货币代码
bond_code	string	持有债券代码
bond_value	double	持有债券市值(元)
bond_quantity	double	持有债券数量（张）
bond_valueto_nav	double	持有债券市值占基金净值比例(%)
update_flag	Int	数据更新标记"""


def extract_table(table):
    return pd.DataFrame([line.split("\t") for line in table.split("\n")])[0].tolist()


if __name__ == '__main__':
    print(extract_table(TABLE))

