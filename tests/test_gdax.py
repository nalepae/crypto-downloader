"""Test GDAX broker."""
import src.download_trades as dwnld

import pytest
import requests


def test_get_trades():
    """Test get_trades."""
    trades = dwnld.GDAX.get_trades(1, 'BTC-EUR')

    assert trades[0][0] == {'price': 204.0,
                            'time': u'2015-04-29T07:31:37.989314Z',
                            'side': u'sell',
                            'trade_id': 100,
                            'size': 0.048}

    assert len(trades[0]) == dwnld.GDAX.LIMIT
    assert not trades[1]

    trades = requests.get(dwnld.GDAX.BASE_URL + 'BTC-EUR' + '/trades')
    last_trade = trades.json()[0]['trade_id']

    trades = dwnld.GDAX.get_trades(last_trade - 20, 'BTC-EUR')
    assert len(trades[0]) == 21
    assert trades[1]


def test_get_last_trade_id_of_file():
    """Test get_last_trade_id_of_file."""
    tid = dwnld.GDAX.get_last_trade_id_of_file('tests/data/gdax/BTC-EUR.csv')
    assert tid == 26

    tid = dwnld.GDAX.get_last_trade_id_of_file('tests/data/nothing.csv')
    assert tid == 0

    with pytest.raises(ValueError):
        dwnld.GDAX.get_last_trade_id_of_file('tests/data/gdax/BTC-EUR_bad.csv')


def test_check_file_consistency():
    """Test check_file_consistency."""
    file = 'tests/data/gdax/BTC-EUR_non_cont.csv'
    diff = dwnld.GDAX.check_file_consistency(file)
    assert diff == [13, 5, 4, 9, 14]

    diff = dwnld.GDAX.check_file_consistency('tests/data/gdax/BTC-EUR.csv')
    assert diff == []
