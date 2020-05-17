#!/usr/bin/env python

"""This program is useful to download all trades from Coinbase.

It sends the data to a Influx Database.
"""
import itertools
import os
from collections import OrderedDict, defaultdict
from datetime import datetime
from importlib import import_module
from inspect import isclass
from pathlib import Path
from typing import Any, Dict, List, Type

import click
from influxdb_client import InfluxDBClient
from influxdb_client.client.flux_table import FluxRecord
from influxdb_client.client.write_api import SYNCHRONOUS

from crypto_data.utils.broker import Broker


def get_broker_name_2_broker() -> Dict[str, Type[Broker]]:
    brokers_directory = import_module("crypto_data.brokers", "crypto_data").__path__[0]  # type: ignore

    files_stem = (entry.stem for entry in Path(brokers_directory).iterdir())

    modules = (
        import_module(f"crypto_data.brokers.{file_stem}", "crypto_data")
        for file_stem in files_stem
    )

    return {
        class_name: broker
        for class_name, broker in itertools.chain.from_iterable(
            (
                (
                    (class_name, getattr(module, class_name))
                    for class_name in dir(module)
                    if isclass(getattr(module, class_name))
                    and issubclass(getattr(module, class_name), Broker)
                    and getattr(module, class_name) != Broker
                )
                for module in modules
            )
        )
    }


def measurements(records: List[FluxRecord],) -> Dict[str, List[Dict[str, Any]]]:
    temp: Dict[str, Dict[datetime, Dict[str, Any]]] = defaultdict(lambda: OrderedDict())

    for record in records:
        if (
            not temp[record.get_measurement()]
            or not temp[record.get_measurement()][record.get_time()]
        ):
            temp[record.get_measurement()][record.get_time()] = {
                record.get_field(): record.get_value()
            }
        else:
            temp[record.get_measurement()][record.get_time()][
                record.get_field()
            ] = record.get_value()

    measurement_name2measurement = defaultdict(list)

    for measurement_name, measurement_time2measurement in temp.items():
        for measurement_time, measurement in measurement_time2measurement.items():
            measurement_name2measurement[measurement_name].append(
                dict(time=measurement_time, **measurement)
            )

    return measurement_name2measurement


broker_name2pairs = {
    broker_name: sorted(broker.get_pairs())  # type: ignore
    for broker_name, broker in get_broker_name_2_broker().items()
}

help_str = f"""
Download all trades since the beginning for a given broker.

The `INFLUXDB_TOKEN` environment variable has to be set with the Influx Database token.

\b
INFLUXDB_ORGANISATION: Influx Database organisation
INFLUXDB_BUCKET: Influx Database bucket

If `PAIRS` is not specified, all the available pairs for the given brocker will be
downloaded.

Here is the list of all available brokers and pairs:\n
{broker_name2pairs}
"""


@click.command(help=help_str)
@click.argument("influxdb_organisation")
@click.argument("influxdb_bucket")
@click.argument("broker")
@click.argument("pairs", nargs=-1, default=None)
@click.option(
    "--influxdb-ip",
    "-i",
    help="Influx Database IP address",
    default="localhost",
    show_default=True,
)
@click.option(
    "--influxdb-port",
    "-p",
    help="Influx Database port",
    default="9999",
    show_default=True,
)
def main(
    influxdb_organisation: str,
    influxdb_bucket: str,
    broker: str,
    influxdb_ip: str,
    influxdb_port: str,
    pairs: List[str],
):
    try:
        influxdb_token = os.environ["INFLUXDB_TOKEN"]
    except KeyError:
        click.secho(
            f"⚠️  Error: The `INFLUXDB_TOKEN` environment variable is not set. "
        )
        return
    client = InfluxDBClient(
        url=f"http://{influxdb_ip}:{influxdb_port}", token=os.environ["INFLUXDB_TOKEN"],
    )

    write_client = client.write_api(write_options=SYNCHRONOUS)
    query_client = client.query_api()

    broker_obj = get_broker_name_2_broker()[broker]

    # TODO: Remove this ignore
    allowed_pairs = broker_obj.get_pairs()  # type: ignore

    if not set(pairs).issubset(allowed_pairs):
        click.secho(
            f"⚠️  Error: For the broker {broker}, "
            f"pair should be one of: {', '.join(sorted(allowed_pairs))}"
        )

        return

    for pair in pairs or sorted(allowed_pairs):
        # Get the last recorded point
        query = f"""
            from(bucket: "{influxdb_bucket}")
            |> range(start: 0)
            |> filter(fn: (r) => r["_measurement"] == "{pair}")
            |> last()
        """

        last = measurements(query_client.query_stream(query, influxdb_organisation))
        last_record = last[pair][0] if last else None

        for trades in broker_obj.get_batches_of_trades(pair, last_record):  # type: ignore
            points = [
                dict(
                    measurement=pair, time=trade.pop("time"), fields=trade, tags=dict()
                )
                for trade in trades
            ]

            if points:
                write_client.write("crypto-data", "Octopus AI", points)


if __name__ == "__main__":
    main()
