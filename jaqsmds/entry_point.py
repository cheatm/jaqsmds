from jaqsmds.server.data_server import start_service
from jaqsmds.config import server_config
import logging
import click


group = click.Group("jaqsmds")


@group.command()
@click.option("--frontend", "-f", default=None)
@click.option("--backend", "-b", default=None)
@click.option("--mongodb_url", "-u", default=None)
@click.option("--process", "-p", default=None, type=click.INT)
@click.option("--log_dir", "-d", default=None)
@click.option("--level", "-l", default=None, type=click.INT)
@click.option("--timeout", "-t", default=None, type=click.INT)
def server(**kwargs):

    for key, value in kwargs.items():
        if value is not None:
            server_config[key] = value

    start_service(**server_config)


if __name__ == '__main__':
    group()