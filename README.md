# Crypto Downloader
This program downloads all trades from Coinbase the beginning.
It writes the result into a InfluxDB 2 database.

If the InfluxDB 2 database already contains some trades, only missing trades will be downloaded.

Because there is a lot of trades, the whole process could take several days.

## Installation
`$ pip install crypto-data`

## Usage
`$ crypto-data INFLUXDB_ORGANISATION INFLUXDB_BUCKET BROKER [PAIRS]...`, where:
* **INFLUXDB_ORGANISATION** is the Influx Database organisation name
* **INFLUXDB_BUCKET** is the Influx Database bucket name
* **BROKER** is the broker used to download data (Example: *Coinbase*)
* **PAIRS** is the list of paris to download. (Example: *BTC-EUR EOS-BTC*). If **PAIRS** is not specified, all the available pairs for the given brocker will be downloaded.