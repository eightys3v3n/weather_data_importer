from csv import DictReader
from itertools import zip_longest
import logging


def csv_file(path, fields=None):
    f = open(path, 'r')
    reader = DictReader(f, fieldnames=fields)
    return reader


def line_count(path):
    lines = 0
    with open(path, 'r') as f:
        for line in f:
            lines += 1
    return lines

def grouper(iterable, n):
    """Given an iterable, it will group the iterable into tuples of length n."""
    args = [iter(iterable)] * n
    return zip_longest(*args)
