#!/usr/bin/env python3
# coding: utf8
import unittest
import os
from collections import namedtuple
import dataclasses 
from dataclasses import dataclass, field, Field
from decimal import Decimal
from datetime import datetime, date, time
from ntlite import CastPy
class TestNtLite(unittest.TestCase):
    def setUp(self): pass
    def tearDown(self): pass
    def test_to_sql_bool_false(self):
        self.assertEqual(0, CastPy.to_sql(False))
    def test_to_sql_bool_true(self):
        self.assertEqual(1, CastPy.to_sql(True))
    def test_to_sql_dt_native(self):
        actual = datetime.fromisoformat('2000-01-01 00:00:00')
        self.assertEqual('1999-12-31 15:00:00', CastPy.to_sql(actual))
    def test_to_sql_dt_utc(self):
        actual = datetime.fromisoformat('2000-01-01 00:00:00+00:00')
        self.assertEqual('2000-01-01 00:00:00', CastPy.to_sql(actual))
    def test_to_sql_dt_tokyo(self):
        actual = datetime.fromisoformat('2000-01-01 00:00:00+09:00')
        self.assertEqual('1999-12-31 15:00:00', CastPy.to_sql(actual))
    def test_to_sql_int(self):
        self.assertEqual(0, CastPy.to_sql(0))
        self.assertEqual(1, CastPy.to_sql(1))
        self.assertEqual(-1, CastPy.to_sql(-1))
    def test_to_sql_str(self):
        self.assertEqual('', CastPy.to_sql(''))
        self.assertEqual('aB c', CastPy.to_sql('aB c'))
    def test_to_sql_none(self):
        self.assertEqual(None, CastPy.to_sql(None))
            
if __name__ == '__main__':
    unittest.main()
