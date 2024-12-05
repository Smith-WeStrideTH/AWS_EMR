import logging
from botocore.exceptions import ClientError
from src.utils.consts import *

logger = logging.getLogger(__name__)


class GlueWrapper:
    """Encapsulates AWS Glue actions."""
    def __init__(self, glue_client):
        """
        :param glue_client: A Boto3 Glue client.
        """
        self.glue_client = glue_client

    def create_and_run_crawler(self,
                               crawler_name: str = CRAWLER_NAME,
                               glue_arn: str = GLUE_ARN,
                               database_name: str = DATABASE_NAME,
                               table_prefix: str = TABLE_PREFIX,
                               s3_target: str = S3_OUT_PATH):

        crawler = self.get_crawler(CRAWLER_NAME)

        if crawler is None:
            self.create_crawler(
                name=crawler_name,
                role_arn=glue_arn,
                db_name=database_name,
                db_prefix=table_prefix,
                s3_target=s3_target
            )

        self.start_crawler(CRAWLER_NAME)

    def start_crawler(self, name: str):
        """
        Starts a crawler. The crawler crawls its configured target and creates
        metadata that describes the data it finds in the target data source.

        :param name: The name of the crawler to start.
        """
        try:
            self.glue_client.start_crawler(Name=name)
        except ClientError as err:
            logger.error(
                "Couldn't start crawler %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def get_crawler(self, name: str):
        """
        Gets information about a crawler.

        :param name: The name of the crawler to look up.
        :return: Data about the crawler.
        """
        crawler = None
        try:
            response = self.glue_client.get_crawler(Name=name)
            crawler = response['Crawler']
        except ClientError as err:
            if err.response['Error']['Code'] == 'EntityNotFoundException':
                logger.info("Crawler %s doesn't exist.", name)
            else:
                logger.error(
                    "Couldn't get crawler %s. Here's why: %s: %s", name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        return crawler

    def create_crawler(self,
                       name: str,
                       role_arn: str,
                       db_name: str,
                       db_prefix: str,
                       s3_target: str):
        """
        Creates a crawler that can crawl the specified target and populate a
        database in your AWS Glue Data Catalog with metadata that describes the data
        in the target.

        :param name: The name of the crawler.
        :param role_arn: The Amazon Resource Name (ARN) of an AWS Identity and Access
                         Management (IAM) role that grants permission to let AWS Glue
                         access the resources it needs.
        :param db_name: The name to give the database that is created by the crawler.
        :param db_prefix: The prefix to give any database tables that are created by
                          the crawler.
        :param s3_target: The URL to an S3 bucket that contains data that is
                          the target of the crawler.
        """
        try:
            self.glue_client.create_crawler(
                Name=name,
                Role=role_arn,
                DatabaseName=db_name,
                TablePrefix=db_prefix,
                Targets={'S3Targets': [{'Path': s3_target}]})
        except ClientError as err:
            logger.error(
                "Couldn't create crawler. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
