"""Module providing a function related to date"""
from datetime import (
    timedelta,
    datetime)
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

import time
import re
import boto3

# ------------------------------------------------------------ #


def generate_date_strings():
    """
    Function generate date string
    """
    ed = {{ execution_date }}
    current_time = ed.replace(second=0, microsecond=0, minute=0)

    formatted_date = current_time.isoformat() + ".000"
    previous_hour = (current_time - timedelta(hours=1)).isoformat() + ".000"

    formatted_date_obj = datetime.strptime(formatted_date, "%Y-%m-%dT%H:%M:%S.%f")
    athena_formatted_date = formatted_date_obj.strftime("%Y-%m-%d %H:%M:%S.%f")

    previous_hour_obj = datetime.strptime(previous_hour, "%Y-%m-%dT%H:%M:%S.%f")
    athena_previous_hour = previous_hour_obj.strftime("%Y-%m-%d %H:%M:%S.%f")

    yst_date = ed - (ed - timedelta(days=1))

    return formatted_date, previous_hour, athena_formatted_date, athena_previous_hour


def execute_mssql_query(query_statement: str, 
                        db_conn_id: str, 
                        db_schema: str):
    """
    Function execute query in MSSQL database
    """
    try:
        if re.search(r'(drop|delete)', query_statement, flags=re.IGNORECASE):
            raise Exception("Invalid query: contains 'drop' or 'delete'")
        mssql = MsSqlHook(mssql_conn_id=db_conn_id, schema=db_schema)
        output = mssql.get_records(query_statement)
        return output

    except Exception as e:
        print(e)
        return False


def get_table_name(query):
    """
    Use regular expressions to extract the table name from the query
    """
    table_name = re.findall(r'\b(?:FROM|JOIN)\s+([^\s;]+)', query, re.IGNORECASE)
    return table_name


def convert_query(string):
    """
    Function convert query contains COUNT(*) to *
    """
    return re.sub(r"COUNT\(\*\)", "*", string)


def one_time_query_fetch_first0(qry: str,
                                tmp_loc: str,
                                aws_access_key_id: str,
                                aws_secret_access_key: str,
                                region: str):
    """
    Function execute query in AWS Athena
    """
    restrict_regex = r'^\s*\b(drop|alter)\b'
    mt = re.search(restrict_regex, qry, re.IGNORECASE)
    if mt is not None:
        raise Exception('mistake? %s' % (qry))

    c = boto3.client('athena',
                     aws_access_key_id=aws_access_key_id, 
                     aws_secret_access_key=aws_secret_access_key, 
                     region_name=region
                     )

    res = c.start_query_execution(
        QueryString=qry,
        ResultConfiguration={
            'OutputLocation': tmp_loc,
        }
    )

    qid = res['QueryExecutionId']

    patience = 5
    while patience > 0:
        time.sleep(5)
        res = c.get_query_execution(
            QueryExecutionId=qid
        )

        status = res['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        patience -= 1

    if status != 'SUCCEEDED':
        return -1

    res = c.get_query_results(
        QueryExecutionId=qid
    )

    return int(res['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
