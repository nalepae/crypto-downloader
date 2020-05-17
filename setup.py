from setuptools import setup, find_packages

setup(
    name="crypto-data",
    version="0.1",
    packages=find_packages(),
    install_requires=["click", "influxdb-client", "requests"],
    entry_points="""
        [console_scripts]
        crypto-data=crypto_data.get_trades:main
    """,
)
