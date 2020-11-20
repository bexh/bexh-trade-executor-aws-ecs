import os

from kinesis.consumer import KinesisConsumer
from kinesis.state import DynamoDB
from redis import StrictRedis
import boto3

from src.bet_executer import BetExecutor
from src.logger import LoggerFactory


class Core:
    def _setup(self):
        log_level = os.environ.get("LOG_LEVEL", None)
        self._logger = LoggerFactory().get_logger(name=__name__, log_level=log_level)
        self._stream_name = os.environ.get("INCOMING_KINESIS_STREAM_NAME")
        self._kcl_state_manager_table_name = os.environ.get("KCL_STATE_MANAGER_TABLE_NAME")
        self._endpoint_url = os.environ.get("ENDPOINT_URL")
        host = os.environ.get("REDIS_HOST")
        port = os.environ.get("REDIS_PORT")
        redis_client = StrictRedis(host=host, port=port, db=0)
        output_stream_name = os.environ.get("OUTGOING_KINESIS_STREAM_NAME")
        kinesis_client = boto3.client("kinesis", endpoint_url=self._endpoint_url)
        self._bet_executor = BetExecutor(logger=self._logger, redis_client=redis_client, kinesis_client=kinesis_client, output_stream_name=output_stream_name)

    def run(self):
        try:
            self._setup()

            state = DynamoDB(table_name=self._kcl_state_manager_table_name)
            consumer = KinesisConsumer(stream_name=self._stream_name, state=state)
            for message in consumer:
                self._logger.debug(message)
                self._bet_executor.handle_record(message)

        except Exception as e:
            self._logger.error(f"Core error: {str(e)}")
