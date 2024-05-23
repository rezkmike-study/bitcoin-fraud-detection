import pytz
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import airflow.exceptions
import boto3
from botocore.exceptions import ClientError
from operator import itemgetter
from datetime import datetime, timezone, timedelta
import logging

class S3NoNewObjectsSensor(BaseSensorOperator):
    template_fields = ('bucket_name', 'table_name', 'key', 'timestamp_file_path')

    @apply_defaults
    def __init__(self, bucket_name,  table_name, key, timestamp_file_path, *args, **kwargs):
        super(S3NoNewObjectsSensor, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.key = key
        self.timestamp_file_path = timestamp_file_path
        logging.info(f"bucket, timestamp: {bucket_name}, {timestamp_file_path}")

    def poke(self, context):
        logging.info(f"Starting to poke for new objects in {self.bucket_name} {context['logical_date'] }...\n wait {self.poke_interval} s")
        dtkl = context["logical_date"].astimezone(pytz.timezone("Asia/Kuala_Lumpur"))
        dt = dtkl + timedelta(days=1)
        logging.info(f"date: {dt}||{context['logical_date']}")
        year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        self.prefix = f"{self.key}/{self.table_name}/parquet/year={year}/month={month}/day={day}"
        logging.info(f"prefix: {self.prefix}")

        self.timestamp_file = f'{context["task_instance_key_str"]}-{context["logical_date"].strftime("%Y%m%d-%H%M%S")}.txt'
        self.lock_file = f'{self.timestamp_file_path}/{self.timestamp_file}'
        logging.info(f"{self.lock_file}")

        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)

        latest_object = None
        latest_timestamp = datetime.min.replace(tzinfo=timezone.utc)
        total_objects = 0
        try:
            for page in page_iterator:
                if 'Contents' in page:
                    total_objects += len(page['Contents'])
                    sorted_objects = sorted(page['Contents'], key=itemgetter('LastModified'), reverse=True)
                    if sorted_objects:
                        if sorted_objects[0]['LastModified'] > latest_timestamp:
                            latest_object = sorted_objects[0]
                            latest_timestamp = latest_object['LastModified']

            logging.info(f"Total number of objects: {total_objects}")

            if not latest_object:
                logging.warning(f"No objects found for the {self.prefix} in the bucket.")
                return False

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                logging.error(f"The bucket {self.bucket_name} does not exist.")
                return False
            elif error_code == 'InvalidAccessKeyId' or error_code == 'NoAuthHandlerFound':
                logging.error("Invalid or missing AWS credentials.")
                return False
            else:
                logging.error(f"An unexpected error occurred: {str(e)}")
                return False
        except Exception as e:
            logging.error(f"An unknown error occurred: {str(e)}")
            return False
        logging.info(f"Latest object timestamp: {latest_timestamp}\nlast object {latest_object['Key']} {latest_object['LastModified']}")

        try:
            with open(self.lock_file, 'r') as f:
                last_stored_timestamp_str = f.read().strip()
            last_stored_timestamp = datetime.fromisoformat(last_stored_timestamp_str).replace(tzinfo=timezone.utc)
        except FileNotFoundError:
            last_stored_timestamp = datetime.min.replace(tzinfo=timezone.utc)

        logging.info(f"Last stored timestamp: {last_stored_timestamp}")

        latest_timestamp = latest_timestamp.astimezone(timezone.utc)
        if latest_timestamp <= last_stored_timestamp:
            logging.info("No new objects found. Proceeding to the next task.")
            return True
        else:
            logging.info("New objects found. Updating the timestamp file.")
            with open(self.lock_file, 'w') as f:
                f.write(latest_timestamp.isoformat())
            return False


    def execute(self, context):
        try:
            super(S3NoNewObjectsSensor, self).execute(context)
        except airflow.exceptions.AirflowSensorTimeout as e:
            logging.info(f"timeout since no new objects found.")
            context['ti'].xcom_push(key="timeout_status", value="timeout")
