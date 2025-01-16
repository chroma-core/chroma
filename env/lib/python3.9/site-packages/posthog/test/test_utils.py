import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import six
from dateutil.tz import tzutc

from posthog import utils

TEST_API_KEY = "kOOlRy2QlMY9jHZQv0bKz0FZyazBUoY8Arj0lFVNjs4"
FAKE_TEST_API_KEY = "random_key"


class TestUtils(unittest.TestCase):
    def test_timezone_utils(self):
        now = datetime.now()
        utcnow = datetime.now(tz=tzutc())
        self.assertTrue(utils.is_naive(now))
        self.assertFalse(utils.is_naive(utcnow))

        fixed = utils.guess_timezone(now)
        self.assertFalse(utils.is_naive(fixed))

        shouldnt_be_edited = utils.guess_timezone(utcnow)
        self.assertEqual(utcnow, shouldnt_be_edited)

    def test_clean(self):
        simple = {
            "decimal": Decimal("0.142857"),
            "unicode": six.u("woo"),
            "date": datetime.now(),
            "long": 200000000,
            "integer": 1,
            "float": 2.0,
            "bool": True,
            "str": "woo",
            "none": None,
        }

        complicated = {
            "exception": Exception("This should show up"),
            "timedelta": timedelta(microseconds=20),
            "list": [1, 2, 3],
        }

        combined = dict(simple.items())
        combined.update(complicated.items())

        pre_clean_keys = combined.keys()

        utils.clean(combined)
        self.assertEqual(combined.keys(), pre_clean_keys)

        # test UUID separately, as the UUID object doesn't equal its string representation according to Python
        self.assertEqual(utils.clean(UUID("12345678123456781234567812345678")), "12345678-1234-5678-1234-567812345678")

    def test_clean_with_dates(self):
        dict_with_dates = {
            "birthdate": date(1980, 1, 1),
            "registration": datetime.utcnow(),
        }
        self.assertEqual(dict_with_dates, utils.clean(dict_with_dates))

    def test_bytes(self):
        if six.PY3:
            item = bytes(10)
        else:
            item = bytearray(10)

        utils.clean(item)

    def test_clean_fn(self):
        cleaned = utils.clean({"fn": lambda x: x, "number": 4})
        self.assertEqual(cleaned["number"], 4)
        # TODO: fixme, different behavior on python 2 and 3
        if "fn" in cleaned:
            self.assertEqual(cleaned["fn"], None)

    def test_remove_slash(self):
        self.assertEqual("http://posthog.io", utils.remove_trailing_slash("http://posthog.io/"))
        self.assertEqual("http://posthog.io", utils.remove_trailing_slash("http://posthog.io"))


class TestSizeLimitedDict(unittest.TestCase):
    def test_size_limited_dict(self):
        size = 10
        values = utils.SizeLimitedDict(size, lambda _: -1)

        for i in range(100):
            values[i] = i

            self.assertEqual(values[i], i)
            self.assertEqual(len(values), i % size + 1)

            if i % size == 0:
                # old numbers should've been removed
                self.assertIsNone(values.get(i - 1))
                self.assertIsNone(values.get(i - 3))
                self.assertIsNone(values.get(i - 5))
                self.assertIsNone(values.get(i - 9))
