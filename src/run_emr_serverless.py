import os
import time
import shutil
import argparse
import boto3
import awswrangler as wrangler

from src.processing.emr import EMRServerless
from src.utils.consts import *

parser = argparse.ArgumentParser(
    description='This Script processes COVID-19 Research Data Using Spark')


parser.add_argument('--env',
                    type=str,
                    default=DEFAULT_ENV,
                    dest='env',
                    help="Environment to run spark Job")

parser.add_argument('--input_path',
                    type=str,
                    default=DEFAULT_INPUT_PATH,
                    dest='input_path',
                    help='Local or S3 Path for Spark data')

parser.add_argument('--input_format',
                    type=str,
                    default=DEFAULT_INPUT_FORMAT,
                    dest='input_format',
                    help='File format to read from')

parser.add_argument('--out_path',
                    type=str,
                    default=S3_OUT_PATH,
                    dest='out_path',
                    help='Local or S3 Path to Save Files')

parser.add_argument("--job_role_arn",
                    help="EMR Serverless IAM Job Role ARN",
                    dest='job_role_arn',
                    default=EMR_JOB_ROLE_ARN)

parser.add_argument("--s3_logs_bucket",
                    help="Amazon S3 Bucket to use for logs and job output",
                    dest='s3_logs_bucket',
                    default=S3_LOGS_BUCKET)

if __name__ == "__main__":
    # Require s3 bucket and job role to be provided
    session = boto3.Session(profile_name=AWS_PROFILE)
    client = session.client("emr-serverless", region_name=AWS_REGION)

    args = parser.parse_args()
    serverless_job_role_arn = args.job_role_arn
    s3_logs_bucket_name = args.s3_logs_bucket

    if UPDATE_MODULES:
        if os.path.isfile(MODULE_FILE):
            os.remove(MODULE_FILE)

        if SCRIPT_FILE is not None and SCRIPT_PATH is not None:
            module_file_name = MODULE_FILE.split('.')[0]
            module_file_extension = MODULE_FILE.split('.')[1]
            filename = f'{module_file_name}.{module_file_extension}'
            shutil.make_archive('src', 'zip', '../', 'src')
            wrangler.s3.upload(SCRIPT_FILE, FULL_SCRIPT_PATH, boto3_session=session)
            wrangler.s3.upload(MODULE_FILE, FULL_MODULE_PATH, boto3_session=session)
            
    if UPDATE_ENVIRONMENT:
        if not os.path.isfile(f'../{ENVIRONMENT_FILE}'):
            raise Exception('Build your environment first using Docker: '
                            'DOCKER_BUILDKIT=1 docker build --output . .')

        wrangler.s3.upload(f'../{ENVIRONMENT_FILE}', FULL_ENVIRONMENT_PATH, boto3_session=session)
    # Create and start a new EMRServerless Spark Application
    emr_serverless = EMRServerless(emr_client=client, application_id=APPLICATION_ID)

    print(f"Creating and starting EMR Serverless Spark App")
    emr_serverless.create_application(APPLICATION_NAME, "emr-6.6.0")
    emr_serverless.start_application()

    print(emr_serverless)

    files = wrangler.s3.list_objects(f'{DEFAULT_INPUT_PATH}', boto3_session=session)
    job_dict = {}
    job_run_id = None
    print(f"Running a total of {len(files)} EMR Serverless Jobs")

    for ix, file in enumerate(sorted(files)):
        # Submit a Spark job
        try:
            job_run_id = emr_serverless.run_spark_job(
                name=APPLICATION_NAME,
                script_location=f"{FULL_SCRIPT_PATH}",
                venv_name="environment",
                venv_location=f'{FULL_ENVIRONMENT_PATH}',
                modules_location=f'{FULL_MODULE_PATH}',
                job_role_arn=serverless_job_role_arn,
                arguments=[args.env, file, args.input_format, args.out_path],
                s3_bucket_name=s3_logs_bucket_name,
                wait=False
            )
            print(f"Submitting new Spark job num {ix} and id {job_run_id}")

        except Exception as e:
            print(f'Error while submitting job: \n{e}')

            for job_run_id in job_dict.keys():
                job_status = emr_serverless.cancel_spark_job(job_id=job_run_id)
                print(f'Job {job_run_id} cancelled')

            raise e

        job_dict[job_run_id] = False

    all_done = False
    jobs_completed = 0
    jobs_running = len(job_dict.keys())
    jobs_failed = 0

    while not all_done:

        for job_id in job_dict.keys():
            job_status = emr_serverless.get_job_run(job_id)
            job_done = job_status.get("state") in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

            if job_done:
                job_dict[job_id] = True
                job_state = job_status.get("state")

                if job_state == "SUCCESS":
                    jobs_completed += 1

                if job_state == "FAILED":
                    jobs_failed += 1

                jobs_running -= 1
                all_done = all(job_dict.values())

            print(f"Jobs Running {jobs_running},"
                  f"\nJobs Completed {jobs_completed},"
                  f"\nJobs Failed {jobs_failed}")

        if all_done:
            break

        print('----')

        time.sleep(20)

    print("Done! 👋")