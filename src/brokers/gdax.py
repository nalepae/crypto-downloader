# coding: utf8

"""Implement GDAX broker."""

import os
import pandas as pd
import requests
import sys
import tailer


class GDAX(object):
    """Represents GDAX broker."""

    # URL to retrieve trades
    BASE_URL = 'https://api.gdax.com/products/'
    ALLOWED_PAIRS = ['BTC-USD', 'BTC-EUR', 'BTC-GBP',
                     'BCH-USD', 'BCH-BTC', 'BCH-EUR',
                     'ETH-USD', 'ETH-BTC', 'ETH-EUR',
                     'LTC-USD', 'LTC-BTC', 'LTC-EUR']

    # Max trades retrievable by GDAX on one request
    LIMIT = 100

    @classmethod
    def get_trades(cls, base_trade_number, pair):
        """Return all trades between base_trade_number and
           base_trade_number + 99.

        Positional arguments:
        base_trade_number -- The base trade number
        pair              -- The pair to trade

        Returns a tuple with the following shape
        (list of dictionnaries, boolean)

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

        Raise a Runtime Error if problem during the request.
        """
        after = base_trade_number + cls.LIMIT

        url = cls.BASE_URL + pair + '/trades/'
        response = requests.get(url, params=dict(after=after))

        # Raise if error
        status_code = response.status_code
        if response.status_code != 200:
            message = ("Error code " + str(status_code) +
                       " for base trade number " + str(base_trade_number))
            raise RuntimeError(message)

        trades = response.json()

        # Test if these trades contain the most recent one
        last = not trades[0]['trade_id'] == base_trade_number + cls.LIMIT - 1

        # If it is the case, delete trades older than base_trade_number
        if last:
            filt_trades = [trade for trade in trades
                           if float(trade['trade_id']) >= base_trade_number]
        else:
            filt_trades = trades

        return ([dict(trade_id=trade['trade_id'], size=float(trade['size']),
                      side=trade['side'], price=float(trade['price']),
                      time=trade['time']) for trade in filt_trades], last)

    @classmethod
    def write_trades_from(cls, base_trade_id, file_path, pair):
        """Write in the file 'file_path' all trades from base_trade_id.

        Output file is a CSV file with the following columns:
        trade_id, price, side, volume, date

        Positional arguments:
        base_trade_id -- The ID of the best trade
        file_path     -- The file where trades should be written
        pair          -- The pair to trade
        """
        is_last_trade = False
        current_base_trade = base_trade_id + 1

        write_header = not os.path.isfile(file_path)

        while not is_last_trade:
            current_last_trade = current_base_trade + cls.LIMIT - 1
            msg = ('Get trades ' + str(current_base_trade) + ' to ' +
                   str(current_last_trade) + "...")
            sys.stdout.write(msg)
            sys.stdout.flush()
            try:
                trades, is_last_trade = cls.get_trades(current_base_trade,
                                                       pair)

                current_base_trade += cls.LIMIT
                sys.stdout.write(" OK\n")
                sys.stdout.flush()

                # Return if no trade detected (could happen if this program is
                # called when no new trade is availabled since the last one in
                # the output file)
                if not trades:
                    return

                df = pd.DataFrame.from_records(trades, index='trade_id')
                df.sort_index(inplace=True)
                df.to_csv(file_path, mode='a', header=write_header)
                write_header = False
            except RuntimeError:
                sys.stdout.write(" KO\n")
                sys.stdout.flush()

    @staticmethod
    def get_last_trade_id_of_file(file_path):
        """Return the last trade ID written in the file file_path.

        Positional arguments:
        file_path -- The CSV file to check
        """
        try:
            last_line = tailer.tail(open(file_path, 'r'), 1)[0]
            return int(last_line.split(',')[0])
        except IOError:
            return 0
        except ValueError:
            message = ('The file ' + file_path + ' seems corrupted. Please ' +
                       'take a look.')
            raise ValueError(message)

    @staticmethod
    def check_file_consistency(file_path):
        """Return a list containing the trade ID where a issue is detected in
        the file file_path.

        Positional arguments:
        file_path -- The file to check
        """
        tr_ids = pd.read_csv(file_path, usecols=['trade_id'])
        return tr_ids[tr_ids.diff()['trade_id'] != 1]['trade_id'].tolist()[1:]

    @classmethod
    def print_check_file_consistency(cls, file_path):
        """Check the consistency of the file file_path and print error message
        on console if needed.

        Positional arguments:
        file_path -- The file to check
        """
        diffs = cls.check_file_consistency(file_path)

        if diffs:
            print('Errors detected in file "' + file_path + '" at trades ' +
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
        last_trade = cls.get_last_trade_id_of_file(out_f)

        # Download missing trades
        cls.write_trades_from(last_trade, out_f, pair)
