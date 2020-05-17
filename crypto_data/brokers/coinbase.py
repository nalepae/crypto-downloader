# coding: utf8

"""Implement Coinbase broker."""

from itertools import count
from time import sleep
from typing import Dict as _Dict
from typing import Iterator as _Iterator
from typing import List as _List
from typing import Set as _Set
from typing import Union as _Union
from typing import Any as _Any
from typing import Optional as _Optional

import requests as _requests

from crypto_data.utils.broker import Broker as _Broker

_TRADE = _Dict[str, _Union[str, int, float]]
_TRADES = _List[_TRADE]


class Coinbase(_Broker):
    """Represents Coinbase broker."""

    # URL to retrieve trades
    _BASE_URL = "https://api.pro.coinbase.com/products/"

    # Max trades retrievable by GDAX on one request
    _LIMIT = 100

    @classmethod
    def get_pairs(cls) -> _Set[str]:
        response = _requests.get(cls._BASE_URL)
        response.raise_for_status()

        return {item["id"] for item in response.json()}

    @classmethod
    def _get_batch_of_trades(cls, pair: str, oldest_trade_index: int) -> _TRADES:
        """Return all 100 trades after `oldest_trade_index` included.

        Example: If `oldest_trade_index` = 101, indexes of returned trades will be
        from 101 to 200.

        Returns a tuple of dictionnaries

        The tuple of dictionnaries contains dictionnaries with the following
        shape:
        {
          'time': '2015-04-29T04:55:54.675974Z',
          'trade_id': 42,
          'price': 206.13,
          'size': 0.1037,
          'side': 'sell',
          'last': False
        }

        Raise a HTTPError if problem during the request.
        """

        url = f"{cls._BASE_URL}{pair}/trades/"

        def get_response(url: str, after: int) -> _requests.Response:
            try:
                response = _requests.get(url, dict(after=after))
                response.raise_for_status()
                return response
            except _requests.HTTPError:
                if response.status_code == 429:
                    sleep(1)
                    return get_response(url, after)
                else:
                    raise _requests.HTTPError(
                        f"Error code {response.status_code} for after parameter {after}"
                    )

        return [
            dict(
                trade_id=trade["trade_id"],
                size=float(trade["size"]),
                side=trade["side"],
                price=float(trade["price"]),
                time=trade["time"],
            )
            for trade in get_response(url, oldest_trade_index + cls._LIMIT).json()
            if trade["trade_id"] >= oldest_trade_index
        ]

    @classmethod
    def get_batches_of_trades(
        cls, pair: str, last_record: _Optional[_Dict[str, _Any]]
    ) -> _Iterator[_TRADES]:
        first_trade_index_to_get = last_record["trade_id"] + 1 if last_record else 1

        for batch_index in count(first_trade_index_to_get, cls._LIMIT):
            trades = cls._get_batch_of_trades(pair, batch_index)

            yield trades

            if len(trades) < cls._LIMIT:
                return
