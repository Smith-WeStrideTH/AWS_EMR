import pandas as pd
import awswrangler as wr


class AthenaWrapper:

    def __init__(self, boto3_session=None, database='default'):
        self.session = boto3_session
        self.database = database

    def get_all_data(self, limit=None) -> pd.DataFrame:
        if limit is None:
            sql = "SELECT * FROM covid19_emr_1out"
        else:
            sql = f"SELECT * FROM covid19_emr_1out limit {limit}"

        df = wr.athena.read_sql_query(sql, database=self.database, boto3_session=self.session)
        return df
