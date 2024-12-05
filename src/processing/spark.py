# %%
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataframe


def get_spark_session(env: str = None, app_name: str = 'SparkApp') -> SparkSession:
    if env is not None and env == 'DEV':
        spark = (SparkSession
                 .builder
                 .master('local[*]')
                 .appName(app_name)
                 .getOrCreate())
    else:
        spark = (SparkSession
                 .builder
                 .appName(app_name)
                 .getOrCreate())
    return spark


def from_files(spark: SparkSession, data_dir: str, file_format: str) -> SparkDataframe:
    file = f'{data_dir}'
    df = (spark.read.option("header", True).format(file_format).load(file))
    return df


def to_files(df, tgt_dir: str):
    (df
     .write
     .mode('overwrite')
     .format('parquet')
     .save(tgt_dir))
