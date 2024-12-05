import dataclasses
import sys

from src.processing.spark import get_spark_session, from_files, to_files
from src.processing.transform import run_pipeline

SAMPLE_AMOUNT = None

@dataclasses.dataclass
class InputParameters:
    env: str
    input_path: str
    input_format: str
    out_path: str

    @classmethod
    def get_from_ordered_args(cls, argv):
        if len(argv) != 5:
            raise ValueError('Incorrect number of args, expected ordered arguments is: '
                             'environment, input path, input format, output path')
        return cls(
            env=argv[1],
            input_path=argv[2],
            input_format=argv[3],
            out_path=argv[4]
        )


if __name__ == '__main__':

    params = InputParameters.get_from_ordered_args(sys.argv)

    print(f"Parameters:"
          f"\nEnv: {params.env}"
          f"\nInput Path: {params.input_path}"
          f"\nInput Format: {params.input_format}"
          f"\nOutput Path: {params.out_path}")

    spark = get_spark_session(params.env, 'COVID-19 Research papers dataset')

    spark.sparkContext.setLogLevel('warn')
    log4jLogger = spark._jvm.org.apache.log4j
    logger = log4jLogger.LogManager.getLogger(__name__)
    logger.warn(f"Pyspark script logger initialized from {__name__}")

    df = from_files(spark, params.input_path, params.input_format)

    if SAMPLE_AMOUNT is not None:
        df = df.sample(SAMPLE_AMOUNT)

    df = run_pipeline(df)

    to_files(df, params.out_path)

    print("Done")
