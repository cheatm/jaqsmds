import click


group = click.Group("jaqsmds")


@group.command(help="Run auth server for jaqs.data.DataApi client.")
@click.argument("variables", nargs=-1)
@click.option("-a", "--auth", is_flag=True, default=False)
def server(variables, auth):
    from jaqsmds.server.server import start_service

    env = {}
    for item in variables:
        r = item.split("=")
        if len(r) == 2:
            env[r[0]] = r[1]

    start_service(auth, **env)


def catch_db(string):
    if string:
        return dict(map(lambda s: s.split("="), string.replace(" ", "").split("&")))
    else:
        return {}


if __name__ == '__main__':
    import sys
    # sys.argv.extend("auth_server MONGODB_URI=192.168.0.102 REDIS_URL=192.168.0.102/1".split(" "))
    sys.argv.extend("server MONGODB_URI=192.168.0.102".split(" "))
    group()