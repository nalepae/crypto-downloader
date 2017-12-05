"""Test Kraken broker."""
import src.download_trades as dwnld
import pytest
import requests


def test_get_trades():
    """Test Kraken get_trades."""
    trades = dwnld.Kraken.get_trades(0, 'XBTEUR')

    trade = ["97.00000", "1.00000000", 1378856831.546, "s", "m", ""]
    assert trades[0][0] == trade

    assert len(trades[0]) == dwnld.Kraken.LIMIT
    assert not trades[1]

    response = requests.get(dwnld.Kraken.BASE_URL, params=dict(pair='XBTEUR'))
    last = response.json()['result']['last']

    trades = dwnld.Kraken.get_trades(last, 'XBTEUR')

    assert len(trades) < dwnld.Kraken.LIMIT
    assert trades[1]


def test_get_last_trade_timestamp_of_file():
    """Test get_last_trade_timestamp_of_file."""
    file = 'tests/data/kraken/XBTEUR.csv'
    lttf = dwnld.Kraken.get_last_trade_timestamp_of_file(file)
    assert lttf == 1379367396963000000

    lttf = dwnld.GDAX.get_last_trade_id_of_file('tests/data/nothing.csv')
    assert lttf == 0

    with pytest.raises(ValueError):
        file = 'tests/data/kraken/XBTEUR_bad.csv'
        dwnld.GDAX.get_last_trade_id_of_file(file)


def test_check_file_consistency():
    """Test check_file_consistency."""
    file = 'tests/data/kraken/XBTEUR_non_cont.csv'
    diff = dwnld.Kraken.check_file_consistency(file)
    assert diff == [1379071113451500000, 1379177019796700000]

    diff = dwnld.Kraken.check_file_consistency('tests/data/kraken/XBTEUR.csv')
    assert diff == []
