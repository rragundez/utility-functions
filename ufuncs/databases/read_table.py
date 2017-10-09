from contextlib import contextmanager

from data_collector import DataCollector


@contextmanager
def open_table(*, table, schema=None, **kwargs):
    collector = DataCollector(table, schema, **kwargs)
    try:
        yield collector
    finally:
        collector.close()
