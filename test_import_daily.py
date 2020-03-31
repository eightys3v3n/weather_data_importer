from unittest import TestCase
from import_daily import *
from datetime import date


logging.basicConfig(level=logging.INFO)


class TestImportDaily(TestCase):
    def test_parse_row(self):
        i = {'date': "2020-02-23",
             'min_temperature': "-10.0",
             'max_temperature': "10.1",
             'avg_hourly_temperature': "2.5",
             'min_windchill': '-4.2'
        }
        c = {'date': date(2020, 2, 23),
             'min': -10.0,
             'max': 10.1,
             'avg_hourly': 2.5,
             'windchill': -4.2,
        }
        o = parse_row(i)
        self.assertEqual(o, c)
