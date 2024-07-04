from __future__ import annotations

from datetime import date, datetime
from os import getenv
from time import time
from typing import TYPE_CHECKING

import polars as pl
from polars import DataFrame
from yass import EntryPoint, contentio
from yass.resources.api import EndpointPath, SimpleEORTrigger
from yass.scheduling import TimeCondition
from yass.storages import blob

__all__ = [
    "API_KEY",
    "API_URL",
    "HEADERS",
    "S3_KWARGS",
    "TICKERS",
    "append_interest_rate_ratio",
    "get_only_income_ratio",
    "insert_metadata",
]

if TYPE_CHECKING:
    from yass.resources import ApiRequest, ApiResource
    from yass.scheduling.base import ActionCondition
    from yass.storages.blob import FsBlobStorage

API_KEY: str | None = getenv("FMP_API")
API_URL = "https://financialmodelingprep.com/api/v3"
S3_KWARGS = {
    "endpoint_url": getenv("S3_ENDPOINT"),
    "key": getenv("S3_KEY"),
    "secret": getenv("S3_SECRET"),
    "client_kwargs": {"region_name": getenv("S3_REGION")},
}

TICKERS: list[str] = ["AAPL", "MSFT", "AMZN", "NVDA", "TSLA", "MA", "PG"]
HEADERS: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Accept-Encoding": "gzip, deflate, br, zstd",
}

app = EntryPoint()
fmp_api: ApiResource = app.define_resource(resource_type="api", url=API_URL)
fmp_api.request_timeout = 60
fmp_api = fmp_api.configure(
    extra_headers=HEADERS,
    max_batch=2,
    delay=5,
    eor_triggers=[SimpleEORTrigger(12)],
)

income_endpoint: EndpointPath = fmp_api.add_endpoint("income")
income_endpoint.add_fix_part("income-statement", 0)
income_endpoint.add_mutable_parts(TICKERS)

balance_endpoint: EndpointPath = fmp_api.add_endpoint("balance")
balance_endpoint.add_fix_part("balance-sheet-statement")
balance_endpoint.add_mutable_parts(TICKERS, prior=1)
print(balance_endpoint.last_prior)

storage: FsBlobStorage = app.define_storage(storage_type="blob")
storage = storage.configure(
    engine_proto="s3",
    engine_params=S3_KWARGS,
    bufferize=True,
    limit_type="count",
    limit_capacity=3,
)

print(storage.registred_types)

contentio.create_datatype(
    format_name="json",
    input_func=pl.read_json,
    output_func=pl.DataFrame.write_json,
)

contentio.create_datatype(
    format_name="csv",
    input_func=pl.read_csv,
    output_func=pl.DataFrame.write_csv,
)

income_context: contentio.IOContext = contentio.create_io_context(
    in_format="json", out_format="csv", storage=storage
)

balance_context: contentio.IOContext = contentio.create_io_context(
    in_format="json", out_format="csv", storage=storage
)

i_pathgenerator: contentio.PathTemplate = (
    income_context.attache_pathgenerator()
)
i_pathgenerator.add_segment("_", 1, ["fmp_api"])
i_pathgenerator.add_segment("_", 2, ["income", date.today, time])
i_pathgenerator.add_segment("_", 3, ["test"])

b_pathgenerator: contentio.PathTemplate = (
    balance_context.attache_pathgenerator()
)
b_pathgenerator.add_segment("_", 1, ["fmp-api"])
b_pathgenerator.add_segment("_", 2, ["balance", date.today, time])
b_pathgenerator.add_segment("_", 3, ["test"])

i_pathgenerator.render_path()

income_pipeline: contentio.IOBoundPipeline = income_context.attache_pipeline()


@income_pipeline.step(1)
def append_interest_rate_ratio(data: DataFrame) -> DataFrame:
    new_frame = data.with_columns(
        (pl.col("interestExpense") / pl.col("revenue") * 100)
        .round(2)
        .alias("InterestRate")
    )
    return new_frame


@income_pipeline.step(2)
def get_only_income_ratio(data: DataFrame) -> DataFrame:
    needed_fields = [
        "date",
        "symbol",
        "fillingDate",
        "calendarYear",
        "period",
        "grossProfitRatio",
        "operatingIncomeRatio",
        "netIncomeRatio",
        "InterestRate",
    ]
    new_frame = data.select(needed_fields)
    return new_frame


@income_pipeline.step(3)
def insert_metadata(data: DataFrame) -> DataFrame:
    created_at = pl.Series(
        "created_at", [datetime.now().date()] * data.shape[0], pl.Date
    )
    new_frame = data.insert_column(data.shape[1], created_at)
    return new_frame


income_pipeline.show_pipeline()

time_interval_income: ActionCondition = TimeCondition(
    period=1, start_time="11:22", end_time="22:00", frequency=0
)
time_interval_balance: ActionCondition = TimeCondition(
    period=1, start_time="11:22", end_time="22:00", frequency=0
)

report_params = {"period": "annual", "limit": 5, "apikey": API_KEY}

income_query: ApiRequest = fmp_api.make_query(
    "income_statement",
    "income",
    fix_params=report_params,
    collect_interval=time_interval_income,
    io_context=income_context,
    has_pages=False,
)

balance_query: ApiRequest = fmp_api.make_query(
    "balance_sheet",
    balance_endpoint,
    fix_params=report_params,
    collect_interval=time_interval_balance,
    io_context=balance_context,
    has_pages=False,
)

content_list = blob.ls_storage(storage.engine, "fmp-api")
print(content_list)
content = blob.read(storage.engine, content_list[0])
print(content)

# app.run()
