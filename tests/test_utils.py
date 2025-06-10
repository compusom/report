import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import numpy as np
from utils import robust_numeric_conversion


def test_us_format():
    assert robust_numeric_conversion("1,234.56") == 1234.56


def test_eu_format():
    assert robust_numeric_conversion("1.234,56") == 1234.56


def test_currency_and_sign():
    assert robust_numeric_conversion("$ -100") == -100.0


def test_invalid_returns_nan():
    res = robust_numeric_conversion("abc")
    assert isinstance(res, float) and np.isnan(res)

