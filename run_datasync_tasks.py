import os
import sys
import json
import argparse
import mysql.connector
import threading
import time
import redis
import logging
import boto3

from datetime import datetime
from random import randint

from dotenv import load_dotenv

parser = argparse.ArgumentParser()

parser.add_argument(
    "-e",
    "--env_file",
    required=False,
    default=".env",
    help="Specify the .env file to load environment variables from. The default is .env.",
)

parser.add_argument(
    "-t",
    "--tasks_count",
    type=int,
    default=1,
    help="Define number of PHP parallel threads.",
)

args = parser.parse_args()
env_file = args.env_file
tasks_count = args.tasks_count

# Load environment variables from .env file
load_dotenv(env_file)

QUEUE_NAME = os.environ.get("QUEUE_NAME", "datasync_instances")
COMPLETED_INSTANCES_QUEUE = os.environ.get(
    "COMPLETED_INSTANCES_QUEUE", "datasync_completed_instances"
)
DEAD_LETTER_QUEUE = os.environ.get(
    "DEAD_LETTER_QUEUE", "datasync_dead_letter_queue"
)
RUNNING_SET = os.environ.get("RUNNING_SET", "datasync_running_instances")
TASK_RUNNING_SET = os.environ.get("TASK_RUNNING_SET", "datasync_running_tasks")


INSTANCE_KEY_PREFIX = os.environ.get("INSTANCE_KEY_PREFIX", "datasync_instance_")
TASK_KEY_PREFIX = os.environ.get("TASK_KEY_PREFIX", "datasync_task_")
LOCK_KEY_PREFIX = os.environ.get("LOCK_KEY_PREFIX", "datasync_lock:")
REDIS_HOST = os.environ.get("REDIS_HOST", "")

# Redis credentials
REDIS_PORT = os.environ.get("REDIS_PORT", "")

# Metadb credentials
meta_db_host = os.environ.get("MYSQL_HOST")
meta_database = os.environ.get("MYSQL_DATABASE")
meta_db_user = os.environ.get("MYSQL_DB_USER")
meta_db_password = os.environ.get("MYSQL_DB_PASSWORD")

# Task name
TASK_NAME = os.environ.get("TASK_NAME")
REGION = os.environ.get("REGION")
SOURCE_EFS_ID = os.environ.get("SOURCE_EFS_ID")
SOURCE_SUBNET_ID = os.environ.get("SOURCE_SUBNET_ID")
SECURITY_GROUP = os.environ.get("SECURITY_GROUP")
SOURCE_SUBDIRECTORY = os.environ.get("SOURCE_SUBDIRECTORY")
DEST_LOCATION_ARN = os.environ.get("DEST_LOCATION_ARN")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID")

# Log directory
log_directory = os.environ.get("LOG_DIRECTORY", "log/")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

date_string = datetime.now().strftime("%Y%m%d")
log_folder = f"{log_directory}{date_string}/"
os.makedirs(log_folder, exist_ok=True)

log_stdout_handler = logging.StreamHandler(sys.stdout)
log_stdout_handler.setLevel(logging.INFO)
log_stdout_handler.setFormatter(formatter)


timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
error_log_handler = logging.FileHandler(f"{log_folder}error-{timestamp}.log")
error_log_handler.setLevel(logging.ERROR)
error_log_handler.setFormatter(formatter)

log_file_handler = logging.FileHandler(f"{log_folder}info-{timestamp}.log")
log_file_handler.setLevel(logging.DEBUG)
log_file_handler.setFormatter(formatter)


# logger.addHandler(log_file_handler)
logger.addHandler(log_stdout_handler)
logger.addHandler(log_file_handler)
logger.addHandler(error_log_handler)


# Instance datasync status logging
failed_instances_file = f"{log_directory}failed_instances.txt"
completed_instances_file = f"{log_directory}completed_instances.txt"
running_instances_file = f"{log_directory}running_instances.txt"

# Redis client
redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, socket_timeout=10, retry_on_timeout=True
)
REGION = os.environ.get("REGION", "us-east-1")
datasync = boto3.client("datasync", region_name=REGION)

class DatasyncConfig:
    def __init__(self, config_file_name="datasync_config.json"):
        self.config_file_name = config_file_name

    def get_queue_name(self, default_value=QUEUE_NAME):
        with open(self.config_file_name, "r") as config_file:
            data = json.load(config_file)
            return data.get("queue_name", default_value)

    def get_tasks_count(self, default_value=3):
        with open(self.config_file_name, "r") as config_file:
            data = json.load(config_file)
            return data.get("tasks_count", default_value)

    def get_stop_flag(self):
        with open(self.config_file_name, "r") as config_file:
            data = json.load(config_file)
            return data.get("stop_datasync", 0)


class MySQLDatabase:
    def __init__(
        self,
        host=meta_db_host,
        user=meta_db_user,
        password=meta_db_password,
        database=meta_database,
        port=3306,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            port=self.port,
            database=self.database,
        )
        self.cursor = self.connection.cursor()

        return self

    def disconnect(self):
        if self.connection.is_connected():
            try:
                self.cursor.close()
                self.connection.close()
            except Exception as e:
                logging.error(f"Error disconnecting from the database {self.host}: {e}")

    def select_all(self, query, as_dict=True):
        try:
            self.cursor.execute(query)
            if as_dict:
                col_description = {
                    index: col[0] for index, col in enumerate(self.cursor.description)
                }
                for row in self.cursor.fetchall():
                    yield {
                        col_description.get(index): row[index]
                        for index in range(len(row))
                    }
            else:
                for row in self.cursor.fetchall():
                    yield row
        except Exception as e:
            logging.error(f"Error selecting data from MySQL: {e}")
            return []

    def select_one(self, query):
        return [row for row in self.select_all(query)][0]

    def run_query(self, query, params=()):
        self.cursor.execute(query, params)
        self.connection.commit()

    def run_select_one(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()


class DatabasaManager:
    def get_meta_db(self):
        return MySQLDatabase().connect()

    def get_instance_db(self, instance):
        instance_id = instance.get("id")
        try:
            instance_db = MySQLDatabase(
                host=instance.get("database_server_replica")
                or instance.get("database_server"),
                user=instance.get("database_user"),
                password=instance.get("database_password"),
                database=instance.get("database_name"),
            ).connect()
            return instance_db
        except Exception as e:
            logging.warning(
                f"instance-{instance_id}-datasync: Failed to connecto to the replica database.",
                exc_info=e,
            )
            return None

    def get_primary_instance_db(self, instance):
        instance_id = instance.get("id")
        try:
            instance_db = MySQLDatabase(
                host=instance.get("database_server"),
                user=instance.get("database_user"),
                password=instance.get("database_password"),
                database=instance.get("database_name"),
            ).connect()
            return instance_db
        except Exception as e:
            logging.warning(
                f"instance-{instance_id}-datasync: Failed to connecto to primary database.",
                exc_info=e,
            )
            return None

class InstancesRepository:
    def __init__(self):
        self.meta_db = DatabasaManager().get_meta_db()

    def get_instances_from_meta(self):
        instance_query = f"""SELECT
                i.id AS id,
                i.is_active,
                i.hostname,
                i.database_name,
                i.database_user,
                i.database_password,
                i.cassandra_schema_version,
                ds.ip AS database_server,
                ds.id AS database_server_id,
                fs.attachment_path as attachment_path,
                fs.id as file_server_id
            FROM
                instances i
            JOIN database_servers ds ON
                i.database_server_id = ds.id
            JOIN file_servers fs ON
                i.file_server_id = fs.id
            ORDER BY
                backup_file_size  ASC
        """
        try:
            self.instances = [
                instance for instance in self.meta_db.select_all(instance_query)
            ]
            self.meta_db.disconnect()
            return self.instances
        except Exception as e:
            logging.error("Failed to connect to MySQL:", exc_info=e)
            exit()

class TasksManager:
    def __init__(self, tasks_count, source_efs_id):
        self.tasks_count = tasks_count
        self.dest_loc = DEST_LOCATION_ARN
        self.source_loc = self.get_or_create_location(source_efs_id, SOURCE_SUBNET_ID, SOURCE_SUBDIRECTORY)
        self.tasks_arn = [self.get_or_create_task(task_id, self.source_loc, self.dest_loc) for task_id in range(self.tasks_count)]
    
    def get_or_create_location(self, efs_id, subnet_id, subdirectory):
        locations = datasync.list_locations(MaxResults=100)['Locations']
        for loc in locations:
            try:
                loc_desc = datasync.describe_location_efs(LocationArn=loc['LocationArn'])
                if subdirectory in loc_desc['LocationUri']:
                    return loc['LocationArn']
            except datasync.exceptions.InvalidRequestException:
                continue

        response = datasync.create_location_efs(
            Ec2Config={
                "SubnetArn": f"arn:aws:ec2:{REGION}:{ACCOUNT_ID}:subnet/{subnet_id}",
                "SecurityGroupArns": [f"arn:aws:ec2:{REGION}:{ACCOUNT_ID}:security-group/{SECURITY_GROUP}"]
            },
            EfsFilesystemArn=f"arn:aws:elasticfilesystem:{REGION}:{ACCOUNT_ID}:file-system/{efs_id}",
            Subdirectory=subdirectory
        )
        logging.info(f"Created EFS location: {response['LocationArn']}")
        return response["LocationArn"]

    def get_or_create_task(self, task_id, source_loc, dest_loc):
        tasks = datasync.list_tasks(MaxResults=100)['Tasks']
        task_name = f"{TASK_NAME}_{task_id}"
        for task in tasks:
            if task['Name'] == task_name:
                return task['TaskArn']
        response = datasync.create_task(
            SourceLocationArn=source_loc,
            DestinationLocationArn=dest_loc,
            Name=task_name,
            Options={"VerifyMode": "POINT_IN_TIME_CONSISTENT"}
        )
        logging.info(f"Created task: {response['TaskArn']}")
        return response['TaskArn']
    
    def start_task_execution(self, task_id , subdir):
        task_arn = self.tasks_arn[task_id]
        include_filter = [
            {"FilterType": "SIMPLE_PATTERN", "Value": f"/{subdir}/*"}
        ]
        response = datasync.start_task_execution(
            TaskArn=task_arn,
            OverrideOptions={"VerifyMode": "POINT_IN_TIME_CONSISTENT"},
            Includes=include_filter
        )
        self.set_task_execution_arn(task_id, response['TaskExecutionArn'])
        return response['TaskExecutionArn']
    
    def get_task_execution_arn(self, task_id):
        execution_arn = redis_client.get(f"{TASK_KEY_PREFIX}execution_arn_{task_id}")
        if not execution_arn:
            logging.info(f"Task execution arn not found for task {task_id}")
            return None
        return execution_arn.decode("utf-8")
    
    def set_task_execution_arn(self, task_id, execution_arn):
        redis_client.set(f"{TASK_KEY_PREFIX}execution_arn_{task_id}", execution_arn)
    
    def release_task_execution_arn(self, task_id):
        redis_client.delete(f"{TASK_KEY_PREFIX}execution_arn_{task_id}")
    
    def get_free_task(self):
        for task_id in range(self.tasks_count):
            task_key = f"{task_id}"
            if redis_client.sismember(TASK_RUNNING_SET, task_key) == 0:
                return task_id
        return None

    def get_task_status(self, task_id):
        execution_arn = self.get_task_execution_arn(task_id)
        try:
            desc = datasync.describe_task_execution(TaskExecutionArn=execution_arn)
            status = desc['Status']
            files_transferred = desc.get('FilesTransferred', 0)
            bytes_transferred = desc.get('BytesTransferred', 0)

            return {
                "status": status,
                "files_transferred": files_transferred,
                "bytes_transferred": bytes_transferred
            }
        except Exception as e:
            logging.error(f"Failed to get task status for task {task_id}: {e}")
            return {"status": "ERROR", "files_transferred": 0, "bytes_transferred": 0}
    
    def acquire_task(self, task_id):
        redis_client.sadd(TASK_RUNNING_SET, f"{task_id}")
    
    def release_task(self, task_id):
        redis_client.srem(TASK_RUNNING_SET, f"{task_id}")

class Datasync:
    def __init__(self):
        self._instances: dict = {
            instance.get("id"): instance
            for instance in InstancesRepository().get_instances_from_meta()
        }
        self.datasync_config = DatasyncConfig()
        self.tasks_manager = TasksManager(self.datasync_config.get_tasks_count(), SOURCE_EFS_ID)

    def create_file_dir(self, file_name):
        file_dir = os.path.dirname(os.path.abspath(file_name))
        os.makedirs(file_dir, exist_ok=True)

    def acquire_lock(self, lockname, value, acquire_timeout=20, lock_timeout=5):
        lock_key = LOCK_KEY_PREFIX + lockname
        end = time.time() + acquire_timeout
        while time.time() < end:
            if redis_client.set(lock_key, value, ex=lock_timeout, nx=True):
                return True
            time.sleep(4)
        current_value = redis_client.get(lock_key)
        current_value = current_value.decode("utf-8") if current_value else None
        logging.error(f"Failed to acquire lock for {lockname}. Current value: {current_value}")
        return False

    def release_lock(self, lockname, value):
        lock_key = LOCK_KEY_PREFIX + lockname
        pipe = redis_client.pipeline(True)
        value_str = f"{value}"
        try:
            pipe.watch(lock_key)
            stored_value = pipe.get(lock_key)
            stored_value = stored_value.decode("utf-8") if stored_value else None
            if stored_value == value_str:
                pipe.multi()
                pipe.delete(lock_key)
                pipe.execute()
                return True
            pipe.unwatch()
        except Exception:
            return False

    def get_lock_value(self, key):
        lock_key = LOCK_KEY_PREFIX + key
        value = redis_client.get(lock_key)
        return value.decode("utf-8") if value else None

    def acquire_lock_on_instance(self, instance, value):
        instance_id = instance.get("id")
        instance_key = f"{INSTANCE_KEY_PREFIX}{instance_id}"
        if redis_client.sismember(RUNNING_SET, instance_id) == 0 and self.acquire_lock(
            instance_key, value=value, lock_timeout=None
        ):
            logging.info(
                f"instance-{instance_id}-datasync: Acquired lock for the instance."
            )
            self.log_instance_datasync_event(
                instance_id,
                f"Acquired lock for the instance.",
            )
            try:
                redis_client.sadd(RUNNING_SET, instance_id)
                self.log_instance_datasync_event(
                    instance_id,
                    f"Added instance into the running set",
                )
                return True
            except Exception as e:
                self.release_lock(instance_key, value)
                logging.error(
                    f"instance-{instance_id}-datasync: Released for the instance. Error {e} "
                )
                self.log_instance_datasync_event(
                    instance_id,
                    f"Released lock for the instance.",
                )
        return False
    
    def get_task_lock_value(self, task_id):
        key = f"{TASK_KEY_PREFIX}{task_id}"
        return self.get_lock_value(key)

    def get_instance_lock_value(self, instance_id):
        key = f"{INSTANCE_KEY_PREFIX}{instance_id}"
        return self.get_lock_value(key)

    def get_instance(self, instance_id):
        instance = self._instances.get(instance_id)
        if instance is None:
            logging.error(
                    f"instance-{instance_id}-datasync: Instance not found in meta."
                )
            self.log_instance_datasync_event(
                instance_id,
                f"Instance not found in Meta",
            )
        return instance

    def log_instance_datasync_event(self, instance_id, message):
        timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_path = f"{log_directory}{instance_id}_datasync_events.log"
        with open(file_path, "a") as file:
            file.writelines([f"{timestr}: {message}\n"])

    def get_instance_from_redis_queue(self):
        instance_id = redis_client.lpop(
            self.datasync_config.get_queue_name(QUEUE_NAME)
        )
        if not instance_id:
            logging.info("Datasync queue is empty.")
            time.sleep(350 + randint(0, 600))
            return None
        instance_id = int(instance_id.decode("utf-8"))
        logging.info(f"instance-{instance_id}-datasync: Received instance from queue.")
        self.log_instance_datasync_event(instance_id, f"Received instance from queue")
        return instance_id

    def run_datasync(self, task_id, instance: dict):
        instance_id = instance.get("id")
        execution_arn = self.tasks_manager.start_task_execution(task_id, instance_id)
        self.log_instance_datasync_event(
                instance_id,
                f"Started task execution: {execution_arn}",
            )
        logging.info(f"Started task execution for '{instance_id}': {execution_arn}")
        return execution_arn

    def datasync_worker(self):
        while (
            self.datasync_config.get_stop_flag() == 0
        ):
            try:
                task_id = self.tasks_manager.get_free_task()
                if task_id is None:
                    logging.info(
                        f"No free tasks found."
                    )
                    time.sleep(10)
                    continue
                
                instance_id = self.get_instance_from_redis_queue()
                if instance_id is None:
                    continue
                instance = self.get_instance(instance_id)
                if instance is None:
                    redis_client.rpush(DEAD_LETTER_QUEUE, instance_id)
                    time.sleep(10)
                    continue

                # Check if the datasync is already running/done for this instance
                if self.acquire_lock_on_instance(instance, task_id) == False:
                    logging.info(
                        f"instance-{instance_id}-datasync: Skipped. Datasync already running or done."
                    )
                    self.log_instance_datasync_event(
                        instance_id,
                        f"Skipped instance - Datasync already running or done",
                    )
                    continue
                self.tasks_manager.acquire_task(task_id)
                try:
                    self.run_datasync(task_id, instance)
                except Exception as e:
                    self.tasks_manager.release_task(task_id)
                    redis_client.rpush(DEAD_LETTER_QUEUE, instance_id)
                    self.log_instance_datasync_event(
                        instance_id,
                        f"Pushed instance into the dead letter queue.",
                    )
            except Exception as e:
                logging.error(e, exc_info=e)
            finally:
                time.sleep(2)

    def check_successful_datasync(self, instance_db):
        pass
        
    def monitor_failed_datasyncs(self):
        while True:
            instance_id = redis_client.lpop(DEAD_LETTER_QUEUE)
            if not instance_id:
                break
            instance_id = int(instance_id.decode("utf-8"))
            with open(failed_instances_file, "a") as file:
                file.write(f"{instance_id}\n")
                logging.info(f"Datasync failed: {instance_id}")
    
    def monitor_finished_datasync(self):
        while True:
            instance_id = redis_client.lpop(COMPLETED_INSTANCES_QUEUE)
            if not instance_id:
                break
            instance_id = int(instance_id.decode("utf-8"))
            with open(completed_instances_file, "a") as file:
                file.write(f"{instance_id}\n")
                logging.info(f"Datasync completed: {instance_id}")

    def monitor_running_datasyncs(self):
        running_set = [
            int(instance_id.decode("utf-8"))
            for instance_id in redis_client.smembers(RUNNING_SET)
        ]
        running_instances = []
        with open(running_instances_file, "w") as file:
            for instance_id in running_set:
                task_id = self.get_instance_lock_value(instance_id)
                task_status = self.tasks_manager.get_task_status(task_id)
                if task_status['status'] not in ['SUCCESS', 'ERROR', 'CANCELED']:
                    file.write(f"ID: {instance_id}, FilesTransferred: {task_status['files_transferred']}, BytesTransferred: {task_status['bytes_transferred']}\n")
                    running_instances.append(instance_id)
                    continue
                
                instance_key = f"{INSTANCE_KEY_PREFIX}{instance_id}"
                logging.info(f"Instance key: {instance_key}, Task ID: {task_id}")
                if task_status['status'] == 'SUCCESS':
                    redis_client.srem(RUNNING_SET, instance_id)
                    redis_client.rpush(COMPLETED_INSTANCES_QUEUE, instance_id)
                    self.tasks_manager.release_task(task_id)
                    self.release_lock(instance_key, task_id)
                    self.log_instance_datasync_event(
                        instance_id,
                        f"Datasync completed: FilesTransferred: {task_status['files_transferred']}, BytesTransferred: {task_status['bytes_transferred']}",
                    )
                else:
                    redis_client.srem(RUNNING_SET, instance_id)
                    redis_client.rpush(DEAD_LETTER_QUEUE, instance_id)
                    self.tasks_manager.release_task(task_id)
                    self.release_lock(instance_key, task_id)
                    self.log_instance_datasync_event(
                        instance_id,
                        f"Datasync Failed: FilesTransferred: {task_status['files_transferred']}, BytesTransferred: {task_status['bytes_transferred']}",
                    )

        logging.info(f"Currently datasync running for: {running_instances}")

    def monitor_bulk_datasync(self):
        logging.info("Monitoring datasyncs")
        while self.datasync_config.get_stop_flag() == 0:
            try:
                self.monitor_failed_datasyncs()
                self.monitor_running_datasyncs()
                self.monitor_finished_datasync()
            except Exception as e:
                logging.error(e, exc_info=e)
            finally:
                time.sleep(30)

    def run(self):
        monitor_thread = threading.Thread(target=self.monitor_bulk_datasync)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        self.datasync_worker()


if __name__ == "__main__":
    Datasync().run()
