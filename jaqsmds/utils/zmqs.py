
BLANK = b""


def not_blank(b):
    return b != BLANK


def _fill_blank(args):
    for value in args:
        yield value
        yield BLANK


def fill_blank(args):
    return list(_fill_blank(args))[:-1]


def del_blank(args):
    return list(filter(not_blank, args))