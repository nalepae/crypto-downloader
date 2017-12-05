"""Test the file resample.py."""
import pytest

import src.resample as resample


def test_load_file():
    """Test load_file."""
    with pytest.raises(RuntimeError):
        resample.load_file('tests/data/gdax/BTC-EUR_bad_header.csv')


def test_resample():
    """Test resample."""
    df = resample.load_file('tests/data/gdax/BTC-EUR.csv')
    re_df = resample.resample(df, '1D')

    assert(re_df.open['2015-04-26'] == 220)
    assert(re_df.high['2015-04-26'] == 220)
    assert(re_df.low['2015-04-26'] == 220)
    assert(re_df.close['2015-04-26'] == 220)
    assert(re_df.volume['2015-04-26'] == 0)

    assert(re_df.open['2015-04-28'] == 209.19)
    assert(re_df.high['2015-04-28'] == 209.53)
    assert(re_df.low['2015-04-28'] == 204.48)
    assert(re_df.close['2015-04-28'] == 205.95)
