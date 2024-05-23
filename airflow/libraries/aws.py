import io
import os
import boto3
from typing import List


class S3:
    def __init__(self, bucket, key=None):
        self.bucket = bucket
        self.key = key

    def upload_file(self, obj=None, path=None, files=None, pattern=None, mode=None):
        s3_client = boto3.client('s3')
        # file_to_ul = []
        # for file in files:
        #     if pattern is not None:
        #         if file.__contains__(pattern):
        #             file_to_ul.append(file)
        #     else:
        #         file_to_ul.append(file)
        # for file in file_to_ul:
        #     # s3_client.upload_file(f'{path}/{file}', self.bucket, f'{self.key}/{file}')
        #     if mode == "file":
        #         s3_client.upload_file(f'{path}/{file}', self.bucket, f'{self.key}/{file}')
        #     elif mode == "obj":
        #         with open(f'{path}/{file}', 'rb') as f:
        #             s3_client.upload_fileobj(f, self.bucket, f'{self.key}/{f}.csv')

        if mode == 'obj':
            s3_client.upload_fileobj(obj, self.bucket, f'{self.key}/{files}.csv')
        elif mode == 'file':
            for file in files:
                if pattern is not None:
                    if file.__contains__(pattern):
                        s3_client.upload_file(file, self.bucket, f'{self.key}/{file}')
                else:
                    s3_client.upload_file(file, self.bucket, f'{self.key}/{file}')

    def upload_dirs(self, dir):
        s3_client = boto3.client('s3')
        uploaded_files = []
        for path, subdirs, files in os.walk(dir):
            # path = path.replace("\\", "/")
            # directory_name = path.replace(path, "")
            for file in files:
                file_to_ul = os.path.join(path, file)
                s3_client.upload_file(file_to_ul, self.bucket, f'{self.key}/{file_to_ul}')
                uploaded_files.append(file_to_ul)
        return uploaded_files
    

class dynamo:
    def __init__(self, table):
        self.table = table

    def retrieve_min_max_ts_and_delete_records(self, primary_key_value):
        # Initialize a DynamoDB client
        dynamodb = boto3.client('dynamodb', region_name='ap-southeast-1')

        # Use the scan operation to read all data from the table
        response = dynamodb.scan(
            TableName=self.table
        )

        min_ts = None
        max_ts = None

        # Delete each item with the specified PrimaryKeyName
        for item in response.get('Items', []):
            key = {
                'PrimaryKeyName': {
                    'S': item['PrimaryKeyName']['S']
                }
                # Add more keys as needed
            }

            # Delete the item if the PrimaryKeyName matches the provided value
            if key['PrimaryKeyName']['S'] == primary_key_value:
                dynamodb.delete_item(
                    TableName=self.table,
                    Key=key
                )
                print(f"Record with PrimaryKeyName '{primary_key_value}' deleted successfully.")

                # Check and update min_ts and max_ts
                item_min_ts = item.get('min_ts', {}).get('S')
                item_max_ts = item.get('max_ts', {}).get('S')

                if item_min_ts and item_max_ts:
                    if min_ts is None or item_min_ts < min_ts:
                        min_ts = item_min_ts
                    if max_ts is None or item_max_ts > max_ts:
                        max_ts = item_max_ts

        if min_ts and max_ts:
            return min_ts, max_ts
        else:
            return None, None
        
    def retrieve_values_and_delete_records(self, primary_key_value: str, target_keys: List[str]):
        """
        This function will only help to get first layer key values pair of dynamo db. The function
        will use the primary key value of the record to look for target records.

        The function will return found record in dictionary format. If there are no items that matched
        the query criteria, and empty dictionary will be returned
        """
        # Initialize a DynamoDB client
        dynamodb = boto3.client('dynamodb', region_name='ap-southeast-1')

        # Query the target table with primary key value
        response = dynamodb.query(
            TableName=self.table,
            KeyConditionExpression="PrimaryKeyName = :PrimaryKeyName",
            ExpressionAttributeValues={
                ':PrimaryKeyName': {'S': primary_key_value}
            }
        )

        result_dict = {}

        # Delete each item with the specified PrimaryKeyName
        for item in response.get('Items', []):
            key = {
                'PrimaryKeyName': {
                    'S': item['PrimaryKeyName']['S']
                }
                # Add more keys as needed
            }

            # Delete the item if the PrimaryKeyName matches the provided value
            if key['PrimaryKeyName']['S'] == primary_key_value:
                dynamodb.delete_item(
                    TableName=self.table,
                    Key=key
                )
                print(f"Record with PrimaryKeyName '{primary_key_value}' deleted successfully.")

                for key in target_keys:

                    field = item.get(key, {})
                    if field=={}:
                        print(f"Target key(`{key}`) not found")
                    else:

                        # Since the function will only focusing on the first layer of the key value
                        # pair of, it will only extract the first value of found key
                        for v in field.values():
                            # It will only take the first record value
                            result_dict[key] = v
                            break

        if result_dict=={}:
            print(f"No records found or match the query criteria ...")
        else:
            return result_dict