#!/usr/bin/env python
# coding: utf8

"""This program is useful to resample data.

The output file is a CSV file with the following shape:
time,open,high,low,close,volume
"""
import argparse
from argparse import RawTextHelpFormatter
import csv
import os
import pandas as pd
import sys


_MANDATORY_COLS = {'price', 'size', 'time'}


def load_file(file_path):
    """Load a CSV file and return a pandas dataframe.

    The input file should be a CSV file with the following data:
    trade_id,price,side,size,time

    trade_id should be a contiguous number.

    Raise ValueError if a issue is detected with trade_id
    Raise RuntimeError if an issue is detected with header

    Positional arguments:
    file_path -- The path of the file to read
    """

    # Check the header
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        header = reader.next()
        if set(header) & _MANDATORY_COLS != _MANDATORY_COLS:
            msg = (file_path + " contain the following header: " +
                   ", ".join(_MANDATORY_COLS))
            raise RuntimeError(msg)

    df = pd.read_csv(file_path, usecols=_MANDATORY_COLS, index_col='time',
                     parse_dates=True)

    return df


def resample(df, period):
    """Resample the data.

    Return a resampled data frame with the following columns:
    time, open, high, low, close, volume

    Positional argument:
    df     -- The pandas data frame to resample
    period -- The resampling period
    """
    resampler = df.resample(period)
    price = resampler['price']
    volume = resampler['size']

    opn = price.first()
    high = price.max()
    low = price.min()
    clo = price.last()

    revol = volume.sum()

    # Resample data frame
    re_df = pd.DataFrame(dict(open=opn, high=high, low=low, close=clo,
                              volume=revol))

    # Replace NaN by correct values for close and volume
    re_df.close.fillna(method='ffill', inplace=True)
    re_df.volume.fillna(0, inplace=True)

    def replace_nan(row):
        """Replace NaN values for open, high and low.

        Positional argument:
        row -- The row to modify
        """

        if pd.isnull(row.open):
            row.open = row.high = row.low = row.close

        return row

    # Replace NaN by correct valued for open, high and low
    re_df.apply(replace_nan, axis=1)

    return re_df


def main():
    """The main function."""
    description = \
        """This program is useful to resample data containing unit trades
        (for example retrieved with 'download_trades_from_gdax') to any period.

        The input CSV file should contain following columns:
        trade_id,price,side,size,time

        The output CSV file will have following columns:
        time,open,high,low,close,volume

        The period argument is a string which should contain a optional number
        followed by one of the following options.

        B       business day frequency
        C       custom business day frequency (experimental)
        D       calendar day frequency
        W       weekly frequency
        M       month end frequency
        SM      semi-month end frequency (15th and end of month)
        BM      business month end frequency
        CBM     custom business month end frequency
        MS      month start frequency
        SMS     semi-month start frequency (1st and 15th)
        BMS     business month start frequency
        CBMS    custom business month start frequency
        Q       quarter end frequency
        BQ      business quarter endfrequency
        QS      quarter start frequency
        BQS     business quarter start frequency
        A       year end frequency
        BA      business year end frequency
        AS      year start frequency
        BAS     business year start frequency
        BH      business hour frequency
        H       hourly frequency
        T       minutely frequency
        S       secondly frequency
        L       milliseonds
        U       microseconds
        N       nanoseconds

        Example: To resample every minute  : freq = 'T' or freq = '1T'
                 To resample every 2 days  : freq = '2D'
                 To resample every 6 months: freq = '6M'
        """

    # Parse CLI arguments
    parser = argparse.ArgumentParser(description=description,
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('input_file', help='Input CSV file')
    parser.add_argument('output_dir',
                        help='Output directory. Will be created if needed')
    parser.add_argument('period', help='Resampling period')
    args = parser.parse_args()

    # Create output directory if needed
    sys.stdout.write('Create output directory if needed... ')
    sys.stdout.flush()
    try:
        os.makedirs(args.output_dir)
    except OSError:
        # The directory already exists. Do nothing special.
        pass
    sys.stdout.write('OK\n')

    # Load the file
    sys.stdout.write('Load the input file... ')
    sys.stdout.flush()
    df = load_file(args.input_file)
    sys.stdout.write('OK\n')

    # Resample the data frame
    sys.stdout.write('Resample... ')
    sys.stdout.flush()
    re_df = resample(df, args.period)
    sys.stdout.write('OK\n')

    # Create the ouput file
    sys.stdout.write('Create the output file... ')
    sys.stdout.flush()
    dum = os.path.splitext(os.path.basename(args.input_file))[0]
    output_file_name = dum + '_' + args.period + '.csv'
    output_file = os.path.join(args.output_dir, output_file_name)
    re_df.to_csv(output_file)
    sys.stdout.write('OK\n')


if __name__ == '__main__':
    main()
