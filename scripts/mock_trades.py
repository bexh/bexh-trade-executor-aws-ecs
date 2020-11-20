import os
import boto3
from json import dumps, load


def mock():
    stream_name = os.environ.get("INCOMING_KINESIS_STREAM_NAME")
    kinesis_client = boto3.client("kinesis", endpoint_url=os.environ.get("ENDPOINT_URL"))
    with open("test/resources/limit-bet/new-limit-bet-multi-execute.json") as f:
        payload = load(f)
    print("writing new bet record")
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=dumps(payload),
        PartitionKey=payload["value"]["event_id"]
    )


if __name__ == "__main__":
    mock()
