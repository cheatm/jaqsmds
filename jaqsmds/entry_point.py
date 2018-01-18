from jaqsmds.server.data_server import start_service
from jaqsmds import config
import os
import click


group = click.Group("jaqsmds")


@group.command(help="Run server for jaqs.data.DataApi client.")
@click.option("--frontend", "-f", default=None)
@click.option("--backend", "-b", default=None)
@click.option("--mongodb_url", "-u", default=None)
@click.option("--process", "-p", default=None, type=click.INT)
@click.option("--log_dir", "-d", default=None)
@click.option("--level", "-l", default=None, type=click.INT)
@click.option("--timeout", "-t", default=None, type=click.INT)
@click.argument("conf", required=False, default="config.json")
def server(conf=None, **kwargs):
    if os.path.isfile(conf):
        config.init_file(conf)

    for key, value in kwargs.items():
        if value is not None:
            config.server_config[key] = value

    start_service(db_map=config.db_map, **config.server_config)


if __name__ == '__main__':
    group()