import subprocess


cmd = """
awslocal kinesis create-stream \
    --stream-name bexh-incoming \
    --shard-count 1
"""
try:
    print(cmd)
    print(subprocess.getoutput(cmd))
except Exception as e:
    print("topic already exists", e)

cmd = """
awslocal kinesis create-stream \
    --stream-name bexh-outgoing \
    --shard-count 1
"""
try:
    print(cmd)
    print(subprocess.getoutput(cmd))
except Exception as e:
    print("topic already exists", e)

cmd = """
awslocal dynamodb create-table \
    --table-name State \
    --attribute-definitions AttributeName=shard,AttributeType=S \
    --key-schema AttributeName=shard,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
"""
try:
    print(cmd)
    print(subprocess.getoutput(cmd))
except Exception as e:
    print("table already exists", e)
