from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


class DB:
    def __init__(self, conn_id, schema=None):
        self.mssql_conn_id = conn_id
        self.schema = schema

    def run_query(self, query, df: bool = False):
        sql = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.schema)
        if df:
            q = sql.get_pandas_df(query)
            result = q.astype(str)
        else:
            result = sql.get_records(query)
        return result
