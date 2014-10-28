from functools import partial
import gevent
from gevent import monkey
from gevent.pool import Pool

monkey.patch_all(thread=False, select=False)


class AsyncInflux(object):

    def __init__(self, method, infdb, **kwargs):
        self.infdb = infdb
        self.method = method
        self.kwargs = kwargs
        self.response = None
        self.exception = None

    def send(self, *kwargs):

        merged_kwargs = {}
        merged_kwargs.update(self.kwargs)
        merged_kwargs.update(kwargs)

        if self.method == "query":
            series = merged_kwargs.get("series")
            querystring = merged_kwargs.get("query")
            try:
                if not series:
                    self.response = self.infdb.query(querystring)
                else:
                    self.response = safe_influx_query(self.infdb, querystring, series)
            except Exception as e:
                self.exception = e

        elif self.method == "write":
            points = merged_kwargs.get("points")
            try:
                self.infdb.write_points(points)
                self.response = True
            except Exception as e:
                self.exception = e

        return self


def send(r, pool=None):

    if pool is not None:
        return pool.spawn(r.send)

    return gevent.spawn(r.send)


write_point = partial(AsyncInflux, 'write')
query = partial(AsyncInflux, 'query')


def map_influx_results(requests, size=None, exception_handler=None):
    requests = list(requests)
    pool = Pool(size) if size else None
    jobs = [send(r, pool) for r in requests]
    gevent.joinall(jobs)


def check_series_exists(infdb, series):
    series_select = "list series /^%s$/" % series
    series_exists = infdb.query(series_select)[0]['points']
    if len(series_exists) > 0:
        return True
    return False


def safe_influx_query(infdb, select_string, series):
    result = [{"columns": [], "points": []}]
    if check_series_exists(infdb, series):
        result = infdb.query(select_string)
    return result
