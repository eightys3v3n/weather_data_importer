from unittest import TestCase
from main import *


class TestModule: pass


class TestMain(TestCase):        
    def test_parse_rows(self):
        module = TestModule()
        def parse_row(row):
            row['1'] = float(row['1'])
            return row    
        module.parse_row = parse_row
        
        in_p = [{'1': '2'}, {'1': '3'}]
        corr = [{'1': 2}, {'1': 3}]

        out_p = parse_rows(in_p, module)

        self.assertEqual(out_p, corr)
