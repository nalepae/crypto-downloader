# coding: utf8

"""Implement Kraken broker."""

import os
import pandas as pd
import requests
import sys
import tailer
import time


class Kraken(object):
    """Represents Kraken broker."""
    BASE_URL = 'https://api.kraken.com/0/public/Trades'
    ALLOWED_PAIRS = [
        'XBTCAD', 'XBTEUR', 'XBTGBP', 'XBTJPY', 'XBTUSD',
        'BCHEUR', 'BCHUSD', 'BCHXBT',
        'DASHEUR', 'DASHUSD', 'DASHXBT',
        'EOSETH', 'EOSXBT',
        'ETCETH', 'ETCEUR', 'ETCUSD', 'ETCXBT',
        'ETHCAD', 'ETHEUR', 'ETHGBP', 'ETHJPY', 'ETHUSD', 'ETHXBT',
        'GNOETH', 'GNOXBT',
        'ICNETH', 'ICNXBT',
        'LTCEUR', 'LTCUSD', 'LTCXBT',
        'MLNETH', 'MLNXBT',
        'REPETH', 'REPEUR', 'REPXBT',
        'USDTUSD',
        'XDGXBT',
        'XLMXBT',
        'XMREUR', 'XMRUSD', 'XMRXBT',
        'XRPEUR', 'XRPUSD', 'XRPXBT',
        'ZECEUR', 'ZECUSD', 'ZECXBT'
    ]

    SPECIAL_PAIRS = dict(XBTCAD='XXBTZCAD', XBTEUR='XXBTZEUR',
                         XBTGBP='XXBTZGBP', XBTJPY='XXBTZJPY',
                         XBTUSD='XXBTZUSD', ETCETH='XETCXETH',
                         ETCEUR='XETCZEUR', ETCUSD='XETCZUSD',
                         ETCXBT='XETCXXBT', ETHCAD='XETHZCAD',
                         ETHEUR='XETHZEUR', ETHGBP='XETHZGBP',
                         ETHJPY='XETHZJPY', ETHUSD='XETHZUSD',
                         ETHXBT='XETHXXBT', ICNETH='XICNXETH',
                         ICNXBT='XICNXXBT', LTCEUR='XLTCZEUR',
                         LTCUSD='XLTCZUSD', LTCXBT='XLTCXXBT',
                         MLNETH='XMLNXETH', MLNXBT='XMLNXXBT',
                         REPETH='XREPXETH', REPEUR='XREPZEUR',
                         REPXBT='XREPXXBT', USDTUSD='USDTZUSD',
                         XDGXBT='XXDGXXBT', XLMXBT='XXLMXXBT',
                         XMREUR='XXMRZEUR', XMRUSD='XXMRZUSD',
                         XMRXBT='XXMRXXBT', XRPEUR='XXRPZEUR',
                         XRPUSD='XXRPZUSD', XRPXBT='XXRPXXBT',
                         ZECEUR='XZECZEUR', ZECUSD='XZECZUSD',
                         ZECXBT='XZECXXBT')

    # Max trades retrievable by Kraken on one request
    LIMIT = 1000

    @staticmethod
    def get_last_trade_timestamp_of_file(file_path):
        """Return the last trade timestamp written in the file file_path.

        Positional arguments:
        file_path -- The CSV file to check
        """
        try:
            last_line = tailer.tail(open(file_path, 'r'), 1)[0]
            return int(last_line.split(',')[3])
        except IOError:
            return 0
        except ValueError:
            message = ('The file ' + file_path + ' seems corrupted. Please ' +
                       'take a look.')
            raise ValueError(message)

    @classmethod
    def get_trades(cls, timestamp, pair):
        """Return up to 1000 trades from base_timestamp

        Positional arguments:
        timestamp -- The timestamp corresponding to the first trade to get
                  -- (in nano-seconds)
        pair      -- The pair to trade

        Returns a tuple with the following shape
        (list of dictionnaries, boolean, next_timestamp)

        The list of dictionnaries contains dictionnaries with the following
        shape:
        {
          'time': '015-04-29T04:55:54.675974Z',
          'trade_id': 42,
          'price': 206.13,
          'size': 0.1037,
          'side': 'sell',
          'last': False
        }

        The boolean is True is this list contain the last known trade, else is
        False.

        next_timestamp is the timestamp to give to the next call to get_trades.

        Raise a Runtime Error if problem during the request.
        """
        response = requests.get(cls.BASE_URL,
                                params=dict(pair=pair, since=timestamp))

        # Raise if error
        status_code = response.status_code
        if response.status_code != 200:
            message = ("Error code " + str(status_code) + " for timestamp " +
                       str(timestamp))
            raise RuntimeError(message)

        res_dic = response.json()

        if res_dic['error']:
            raise ValueError(res_dic['error'])

        special_pair = cls.SPECIAL_PAIRS.get(pair, pair)

        result = res_dic['result']
        trades = result[special_pair]
        next_timestamp = result['last']

        # Test if these trades contain the most recent one
        last = len(trades) != cls.LIMIT

        return trades, last, next_timestamp

    @classmethod
    def write_trades_from(cls, timestamp, file_path, pair):
        """Write in the file 'file_path' all trades from base_trade_id.

        Output file is a CSV file with the following columns:
        trade_id, price, side, volume, date

        Positional arguments:
        timestamp -- The timestamp corresponding to the first trade to get
                  -- (in nano-seconds)
        file_path -- The file where trades should be written
        pair      -- The pair to trade
        """
        is_last_trade = False
        current_timestamp = timestamp
        write_header = not os.path.isfile(file_path)

        while not is_last_trade:
            msg = ('Get trades from timestamp ' + str(current_timestamp) +
                   "...")
            sys.stdout.write(msg)
            sys.stdout.flush()
            try:
                res = cls.get_trades(current_timestamp, pair)
                trades, is_last_trade, next_timestamp = res
                sys.stdout.write(" OK\n")
                sys.stdout.flush()

                # Return if no trade detected (could happen if this program is
                # called when no new trade is availabled since the last one in
                # the output file)
                if not trades:
                    return

                cols = ['price', 'size', 'timestamp', 'side', 'type', 'misc']
                df = pd.DataFrame(trades, columns=cols)
                df.timestamp = df.timestamp * 10**4
                df.timestamp = df.timestamp.astype(int)
                df.timestamp = df.timestamp * 10**5
                df['time'] = pd.to_datetime(df['timestamp'])
                df.iloc[-1, df.columns.get_loc('timestamp')] = next_timestamp
                df.set_index('time', inplace=True)
                df.to_csv(file_path, mode='a', header=write_header)
                write_header = False

                current_timestamp = next_timestamp
            except RuntimeError:
                sys.stdout.write(" KO\n")
                sys.stdout.flush()
            except ValueError:
                sys.stdout.write(" KO (Rate limit exceeded - Wait 3 sec)\n")
                time.sleep(3)
                sys.stdout.flush()

    @staticmethod
    def check_file_consistency(file_path):
        """Return a list containing the trade ID where a issue is detected in
        the file file_path.

        Positional arguments:
        file_path -- The file to check
        """
        tr_ts = pd.read_csv(file_path, usecols=['timestamp'])
        return tr_ts[tr_ts.diff()['timestamp'] < 0]['timestamp'].tolist()

    @classmethod
    def print_check_file_consistency(cls, file_path):
        """Check the consistency of the file file_path and print error message
        on console if needed.

        Positional arguments:
        file_path -- The file to check
        """
        diffs = cls.check_file_consistency(file_path)

        if diffs:
            print('Errors detected in file "' + file_path + '" at lines ' +
                  str(diffs)[1:-1])
            print 'Please fix this file manually'
            return True
        else:
            print 'No error detected in "' + file_path + '"'
            return False

    @classmethod
    def download_missing_trades(cls, out_f, pair):
        """Download the missing trades.

        Positional arguments:
        out_f -- The file where trades should be written
        pair  -- The pair to trade
        """

        # Check output file consistency
        if os.path.isfile(out_f) and cls.print_check_file_consistency(out_f):
            sys.exit()

        # Find the last retrieved trade if exists
        last_trade = cls.get_last_trade_timestamp_of_file(out_f)

        # Download missing trades
        cls.write_trades_from(last_trade, out_f, pair)
