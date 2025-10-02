import os
import redis
import argparse
import json

from dotenv import load_dotenv

load_dotenv()


parser = argparse.ArgumentParser(
    description="Schedules instances for Datasync from a provided file containing instance ids."
)
parser.add_argument(
    "-f",
    "--input_file",
    required=True,
    help="Specify the file path that contains the instance ids list(each line should be an instance id)",
)
args = parser.parse_args()
input_file = args.input_file


REDIS_HOST = os.environ.get("REDIS_HOST", "")
REDIS_PORT = os.environ.get("REDIS_PORT", "")

redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, socket_timeout=10, retry_on_timeout=True
)


def get_queue_name(default_value="datasync_instances"):
    with open("datasync_config.json", "r") as config_file:
        data = json.load(config_file)
        return data.get("queue_name", default_value)


queue_name = get_queue_name(os.environ.get("QUEUE_NAME", "datasync_instances"))


def get_instance_ids():
    ids = []
    with open(input_file, "r") as file:
        for line in file:
            instance_id = line.strip()
            ids.append(instance_id)
    return ids

def push_to_queue(item):
    redis_client.rpush(queue_name, item)
    print(f"Item '{item}' pushed to queue '{queue_name}'")


if __name__ == "__main__":
    for id in get_instance_ids():
        push_to_queue(id)
