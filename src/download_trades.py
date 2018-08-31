#!/usr/bin/env python
# coding: utf8

"""This program is useful to download all trades from GDAX & Kraken.

It writes results in a CSV file.
"""
import argparse
from argparse import RawTextHelpFormatter
import os.path

from brokers.gdax import GDAX
from brokers.kraken import Kraken


def main():
    """The main function."""
    description = """This program is useful to download all trades from GDAX &
        Kraken.

        On GDAX, available pairs are :
        - BTC-USD, BTC-EUR, BTC-GBP
        - BCH-USD, BCH-BTC, BCH-EUR
        - ETH-USD, ETH-BTC, ETH-EUR
        - LTC-USD, LTC-BTC, LTC-EUR

        On Kraken, available pairs are:
        - XBTCAD, XBTEUR, XBTGBP, XBTJPY, XBTUSD
        - BCHEUR, BCHUSD, BCHXBT
        - DASHEUR, DASHUSD, DASHXBT
        - EOSETH, EOSXBT
        - ETCETH, ETCEUR, ETCUSD, ETCXBT
        - ETHCAD, ETHEUR, ETHGBP, ETHJPY, ETHUSD, ETHXBT
        - GNOETH, GNOXBT
        - ICNETH, ICNXBT
        - LTCEUR, LTCUSD, LTCXBT
        - MNLETH, MNLXBT
        - REPETH, REPEUR, REPXBT
        - USDTUSD
        - XDGXBT
        - XLMXBT
        - XMREUR, XMRUSD, XMRXBT
        - XRPEUR, XRPUSD, XRPXBT
        - ZECEUR, ZECUSD, ZECXBT

        It writes the result in a CSV file.

        If the output CSV file already contains some trades, the file WON'T be
        erased, and missing trades will be appened to this file. Of course,
        ONLY missing trades will be downloaded.

        This means you can run this program every day to get last data without
        worring about loosing data or downloading trades you already have.

        Because there is a lot of trades, the whole process could take several
        hours (days?) and the result could lead to a hundreds Mio output file.

        Before exiting, the program runs a check of the output file and
        indicates where it detects an issue.
        """

    # Parse CLI arguments
    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('broker', help='The broker used to download data')
    parser.add_argument('pair', help='The pair to trade')
    parser.add_argument('output_dir',
                        help='Output directory. Will be created if needed')
    args = parser.parse_args()

    broker_str = args.broker
    pair = args.pair
    output_dir = os.path.join(args.output_dir, broker_str)

    brokers = {'GDAX': GDAX, 'Kraken': Kraken}

    if broker_str not in brokers:
        print 'broker should be one of: ' + ', '.join(brokers)
        return

    broker = brokers[broker_str]

    if pair not in broker.ALLOWED_PAIRS:
        print('For the broker ' + broker_str + ', pair should be one of: ' +
              ', '.join(broker.ALLOWED_PAIRS))
        return

    # Create output directory if needed
    try:
        os.makedirs(output_dir)
    except OSError:
        # The directory already exists. Do nothing special.
        pass

    output_file = os.path.join(output_dir, pair + '.csv')

    # Download missing trades
    broker.download_missing_trades(output_file, pair)

    # Check files consistency
    broker.print_check_file_consistency(output_file)


if __name__ == '__main__':
    main()
