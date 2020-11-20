import os
from json import dumps, load, loads

import boto3
import pytest
from redis import StrictRedis

from test.mock_kinesis import MockKinesis
from test.mock_redis import MockRedis
from src.bet_executer import BetExecutor
from src.logger import LoggerFactory, Logger
from src.domain_model import StatusDetails, Bet, ExecutedBets, ExecutedBet


@pytest.fixture
def logger():
    logger = LoggerFactory().get_logger(name=__name__, log_level="DEBUG")
    return logger


@pytest.fixture
def redis_client():
    # host = os.environ.get("REDIS_HOST")
    # port = os.environ.get("REDIS_PORT")
    # redis_client = StrictRedis(host=host, port=port, db=0)
    # return redis_client
    client = MockRedis()
    status_details = StatusDetails(
        status="ACTIVE",
        home_team_abbrev="DEN",
        away_team_abbrev="KAN"
    )
    client.set(name="event:202012060kan", value=str(status_details))
    return client


@pytest.fixture
def kinesis_client():
    # endpoint_url = os.environ.get("ENDPOINT_URL")
    # kinesis_client = boto3.client("kinesis", endpoint_url=endpoint_url)
    # return kinesis_client
    return MockKinesis()


@pytest.fixture
def output_kinesis_stream_name():
    # output_stream_name = os.environ.get("OUTGOING_KINESIS_STREAM_NAME")
    output_stream_name = "test"
    return output_stream_name


def get_records(path: str):
    with open(path) as f:
        datas = load(f)
    records = []
    for data in datas:
        records.append({"Data": dumps(data).encode("utf-8")})
    return records


def get_bet_from_pq(values: list):
    value = next(iter(values), None)
    if not value:
        return None

    data, score = value
    bet = Bet(**loads(data.decode("utf-8")))
    return bet


def compare_bets_on_exchange(expected_output_path: str, redis_client, kinesis_client, team_abbrevs: [str], event_id: str, output_stream_name: str) -> bool:
    with open(expected_output_path) as f:
        exp_details = load(f)
    for team_abbrev in team_abbrevs:
        # Check redis bets
        exp_exchange_bets = list(map(lambda x: Bet(**x), exp_details["redis"][team_abbrev]))
        actual_exchange_bets = []
        exchange_bet = get_bet_from_pq(redis_client.zpopmax(name=f"pq:{event_id}:{team_abbrev}"))
        while exchange_bet:
            actual_exchange_bets.append(exchange_bet)
            exchange_bet = get_bet_from_pq(redis_client.zpopmax(name=f"pq:{event_id}:{team_abbrev}"))
        assert len(exp_exchange_bets) == len(actual_exchange_bets), "Number expected bets does not match"

        for exp_exchange_bet in exp_exchange_bets:
            actual_exchange_bet = list(filter(lambda x: x.bet_id == exp_exchange_bet.bet_id, actual_exchange_bets))[0]
            assert actual_exchange_bet == exp_exchange_bet, "Expected bet on exchange does not match actual"

    # Check kinesis bets
    exp_kinesis_bets = exp_details["kinesis"]

    def convert_to_executed_bets(exp_bet: map):
        executed_bet_list = list(map(lambda x: ExecutedBet(**x), exp_bet["bets"]))
        exp_bet["bets"] = executed_bet_list
        if "execution_time" in exp_bet:
            del exp_bet["execution_time"]
        return ExecutedBets(**exp_bet)

    assert len(exp_kinesis_bets) == len(kinesis_client.streams.get(output_stream_name, [])), "Length of expected kinesis records does not match"
    exp_executed_bets = list(map(convert_to_executed_bets, exp_kinesis_bets))
    actual_executed_bets = list(map(lambda x: convert_to_executed_bets(loads(x)), kinesis_client.streams.get(output_stream_name, [])))
    for i in range(len(exp_executed_bets)):
        exp_bet = exp_executed_bets[i]
        actual_bet = actual_executed_bets[i]
        if (
            exp_bet.event_id == actual_bet.event_id and
            exp_bet.sport == actual_bet.sport and
            exp_bet.odds == actual_bet.odds and
            exp_bet.winning_team_abbrev == actual_bet.winning_team_abbrev and
            len(exp_bet.bets) == len(actual_bet.bets)
        ):
            for j in range(len(exp_bet.bets)):
                if (
                    exp_bet.bets[j].bet_id == actual_bet.bets[j].bet_id and
                    exp_bet.bets[j].brokerage_id == actual_bet.bets[j].brokerage_id and
                    exp_bet.bets[j].user_id == actual_bet.bets[j].user_id and
                    exp_bet.bets[j].amount == actual_bet.bets[j].amount and
                    exp_bet.bets[j].status == actual_bet.bets[j].status
                ):
                    pass
                else:
                    assert False, "Executed bets do not match"
        else:
            assert False, "Executed bets do not match"


def execute_and_compare(
        test_input_path: str,
        test_output_path: str,
        logger: Logger,
        redis_client,
        kinesis_client,
        output_kinesis_stream_name: str
):
    limit_bet_records = get_records(path=test_input_path)
    bet_executor = BetExecutor(
        logger=logger,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        output_stream_name=output_kinesis_stream_name
    )
    for record in limit_bet_records:
        bet_executor.handle_record(record=record)

    compare_bets_on_exchange(
        expected_output_path=test_output_path,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        team_abbrevs=["DEN", "KAN"],
        event_id="202012060kan",
        output_stream_name=output_kinesis_stream_name
    )


@pytest.mark.parametrize(
    "test_input_path,test_output_path",
    [
        ("test/resources/limit-bet/new-limit-bet-basic-execute.json", "test/resources/limit-bet/new-limit-bet-basic-execute-output.json"),
        ("test/resources/limit-bet/new-limit-bet-expired-event.json", "test/resources/limit-bet/new-limit-bet-expired-event-output.json"),
        ("test/resources/limit-bet/new-limit-bet-multi-execute.json", "test/resources/limit-bet/new-limit-bet-multi-execute-output.json"),
        ("test/resources/limit-bet/new-limit-bet-not-better-than-wont-execute.json", "test/resources/limit-bet/new-limit-bet-not-better-than-wont-execute-output.json")
    ]
)
def test_limit_bet(test_input_path, test_output_path, logger, redis_client, kinesis_client, output_kinesis_stream_name):
    execute_and_compare(
        test_input_path=test_input_path,
        test_output_path=test_output_path,
        logger=logger,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        output_kinesis_stream_name=output_kinesis_stream_name
    )


@pytest.mark.parametrize(
    "test_input_path,test_output_path",
    [
        ("test/resources/market-bet/new-market-bet-will-execute.json", "test/resources/market-bet/new-market-bet-will-execute-output.json"),
        ("test/resources/market-bet/new-market-bet-wont-execute.json", "test/resources/market-bet/new-market-bet-wont-execute-output.json"),
        ("test/resources/market-bet/new-market-bet-multi-will-execute.json", "test/resources/market-bet/new-market-bet-multi-will-execute-output.json"),
        ("test/resources/market-bet/new-market-bet-multi-wont-execute.json", "test/resources/market-bet/new-market-bet-multi-wont-execute-output.json")
    ]
)
def test_market_bet(test_input_path, test_output_path, logger, redis_client, kinesis_client, output_kinesis_stream_name):
    execute_and_compare(
        test_input_path=test_input_path,
        test_output_path=test_output_path,
        logger=logger,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        output_kinesis_stream_name=output_kinesis_stream_name
    )


@pytest.mark.parametrize(
    "test_input_path,test_output_path",
    [
        ("test/resources/inactive-event/inactive-event.json", "test/resources/inactive-event/inactive-event-output.json")
    ]
)
def test_inactive_event(test_input_path, test_output_path, logger, redis_client, kinesis_client, output_kinesis_stream_name):
    execute_and_compare(
        test_input_path=test_input_path,
        test_output_path=test_output_path,
        logger=logger,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        output_kinesis_stream_name=output_kinesis_stream_name
    )


@pytest.mark.parametrize(
    "test_input_path,test_output_path",
    [
        ("test/resources/cancel-bet/cancel-bet-basic.json", "test/resources/cancel-bet/cancel-bet-basic-output.json"),
        ("test/resources/cancel-bet/cancel-bet-multiple-with-same-score.json", "test/resources/cancel-bet/cancel-bet-multiple-with-same-score-output.json"),
        ("test/resources/cancel-bet/cancel-bet-not-found.json", "test/resources/cancel-bet/cancel-bet-not-found-ouput.json")
    ]
)
def test_cancel_bet(test_input_path, test_output_path, logger, redis_client, kinesis_client, output_kinesis_stream_name):
    execute_and_compare(
        test_input_path=test_input_path,
        test_output_path=test_output_path,
        logger=logger,
        redis_client=redis_client,
        kinesis_client=kinesis_client,
        output_kinesis_stream_name=output_kinesis_stream_name
    )
