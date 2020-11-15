import os
from json import dumps, load

import boto3
import pytest
from redis import StrictRedis

from main.src.bet_executer import BetExecutor
from main.src.logger import LoggerFactory


@pytest.fixture
def limit_order_records():
    path = "main/test/resources/new-limit-bet.json"
    with open(path) as f:
        datas = load(f)
    records = []
    for data in datas:
        records.append({"Data": dumps(data).encode("utf-8")})
    return records


@pytest.fixture
def logger():
    log_level = os.environ.get("LOG_LEVEL", None)
    logger = LoggerFactory().get_logger(name=__name__, log_level=log_level)
    return logger


@pytest.fixture
def redis_client():
    host = os.environ.get("REDIS_HOST")
    port = os.environ.get("REDIS_PORT")
    redis_client = StrictRedis(host=host, port=port, db=0)
    return redis_client


@pytest.fixture
def kinesis_client():
    endpoint_url = os.environ.get("ENDPOINT_URL")
    kinesis_client = boto3.client("kinesis", endpoint_url=endpoint_url)
    return kinesis_client


@pytest.fixture
def output_kinesis_stream_name():
    output_stream_name = os.environ.get("OUTGOING_KINESIS_STREAM_NAME")
    return output_stream_name


def test_basic_limit_order(limit_order_records, logger, redis_client, kinesis_client, output_kinesis_stream_name):
    bet_executor = BetExecutor(
        logger=logger,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        output_stream_name=output_kinesis_stream_name
    )
    for record in limit_order_records:
        bet_executor.handle_record(record=record)
    assert True
