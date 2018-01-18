from jaqsmds.server.data_server import start_service
from jaqsmds import config
import os
import click
import logging


group = click.Group("jaqsmds")


@group.command(help="Run server for jaqs.data.DataApi client.")
@click.option("--frontend", "-f", default=None, help="Frontend IP which clients connect to.")
@click.option("--backend", "-b", default=None, help="Backend IP which workers connect to.")
@click.option("--mongodb_url", "-u", default=None, help="Mongodb that workers connect to.")
@click.option("--process", "-p", default=None, type=click.INT, help="Number of workers to run.")
@click.option("--log_dir", "-d", default=None, help="Dir where logs redirect to.")
@click.option("--level", "-l", default=None, type=click.INT, help="Logging level, default: %s" % logging.WARNING)
@click.option("--jset", default=None, type=click.STRING, help="DB map for jset.query")
@click.option("--jsd", default=None, type=click.STRING, help="DB map for jsd.query")
@click.argument("conf", required=False, default="config.json")
def server(conf=None, **kwargs):
    # 如发现配置文件，根据配置文件设置
    if os.path.isfile(conf):
        config.init_file(conf)

    # 通过命令行输入设置db映射
    config.update(config.db_map, {"{}.query".format(key): catch_db(kwargs[key]) for key in ("jset", "jsd")})

    # 通过命令行输入配置
    for key, value in kwargs.items():
        if value is not None:
            config.server_config[key] = value

    # 配置完成后启动服务
    start_service(db_map=config.db_map, **config.server_config)


def catch_db(string):
    if string:
        return dict(map(lambda s: s.split("="), string.replace(" ", "").split("&")))
    else:
        return {}


if __name__ == '__main__':
    group()