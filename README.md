# Crypto Downloader

## About

This program downloads all trades from GDAX/Coinbase & Kraken since the beginning.
It writes the result in a CSV file.

Extract of the output file for the pair `BTC-EUR` of GDAX:
```
trade_id,price,side,size,time
1,300.0,sell,0.01,2015-04-23T01:42:34.182104Z
2,200.0,buy,0.01,2015-04-23T05:19:36.540687Z
3,220.0,sell,0.01,2015-04-23T23:27:54.528821Z
4,220.0,sell,0.01,2015-04-24T02:09:47.814117Z
5,205.27,buy,0.0353,2015-04-27T17:45:22.434782Z
```

If the output CSV file already contains some trades, the output file WON'T be erased, and missing trades will be appened to the end.
Of course, ONLY missing trades will be downloaded.

This means you can run this program every day to get last data without worring about loosing data or downloading trades you already have.
Because there is a lot of trades, the whole process could take several hours (days?) and the result could lead to a hundreds Mio output file.

Before exiting, the program runs a check on the output file and prints the trades ID where it detects an issue (missing trade, duplicated trade ...).

## Requirements
* [Pandas](http://pandas.pydata.org)
* [Tailer](https://pypi.python.org/pypi/tailer)

## Usage
`$ ./download_trades_from_gdax BROKER PAIR OUTPUT_DIRECTORY`, where:
* **BROKER** is the broker used to download data
* **PAIR** is the pair to download (example: BTC-EUR)
* **OUPUT_DIRECTORY** is the directory where the output file will be written

# Resampler
To resample downloaded data to periods like hours, days, weeks, months, etc..., please use:

`$ ./resample INPUT_FILE OUTPUT_DIRECTORY PERIOD`, where:
* **INPUT_FILE** is the file containing raw trades
* **OUPUT_DIRECTORY** is the directory where the output file will be written
* **PERIOD** is the perdiod of resampling.

Please use `$ ./resample -h` to get more information about resampling period.

# Already available data
If you want data without download them yourself from Coinbase / GDAX (which could need several days to do it ...), you could visit [this link](https://manunalepa.wordpress.com/2017/11/14/bitcoin-ethereum-litecoin-exchanges-raw-data-from-coinbase-gdax-are-available-here) where you will find all raw data already retrieved for you.

These data are updated every week. So you can download directly all data with [this link](https://manunalepa.wordpress.com/2017/11/14/bitcoin-ethereum-litecoin-exchanges-raw-data-from-coinbase-gdax-are-available-here), and then download missing data with this program.
