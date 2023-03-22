import pypika
from threading import local
import itertools


context = local()


class Value(pypika.Parameter):
    def __init__(self, value):
        self.value = value

    def get_sql(self, **kwargs):
        context.values.append(self.value)
        return context.formatstr.format(next(context.generator))


def build(query, formatstr="?") -> tuple[str, tuple]:
    context.values = []
    context.generator = itertools.count(1)
    context.formatstr = formatstr
    sql = query.get_sql()
    params = tuple(context.values)
    return sql, params
