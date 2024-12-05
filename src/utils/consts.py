
S3_BUCKET = 'project-pro-emr-serverless123'
S3_LOGS_BUCKET = f'projectpro-emr-serverless-logs'

DEFAULT_ENV = 'PROD'
DEFAULT_INPUT_PATH = f's3://{S3_BUCKET}/data'
DEFAULT_INPUT_FORMAT = 'csv'
S3_OUT_PATH = f's3://{S3_BUCKET}/out/'
EMR_JOB_ROLE_ARN = 'arn:aws:iam::143176219551:role/emr-new-role'

CRAWLER_NAME = 'covid-19-crawler'
GLUE_ARN = 'arn:aws:iam::143176219551:role/service-role/AWSGlueServiceRole-EMRServerless'
DATABASE_NAME = 'default'
TABLE_PREFIX = 'covid19_emr_'

SCRIPT_PATH = f's3://{S3_BUCKET}/scripts'
SCRIPT_FILE = 'spark_job.py'
FULL_SCRIPT_PATH = f'{SCRIPT_PATH}/{SCRIPT_FILE}'

ENVIRONMENT_PATH = f's3://{S3_BUCKET}/environment'
ENVIRONMENT_FILE = 'environment.tar.gz'
FULL_ENVIRONMENT_PATH = f'{ENVIRONMENT_PATH}/{ENVIRONMENT_FILE}'

MODULE_PATH = f's3://{S3_BUCKET}/modules'
MODULE_FILE = 'src.zip'
FULL_MODULE_PATH = f'{MODULE_PATH}/{MODULE_FILE}'

AWS_PROFILE = 'projectpro'
AWS_REGION = 'us-east-1'
UPDATE_ENVIRONMENT = False
UPDATE_MODULES = True

APPLICATION_NAME = 'COVID1912'
APPLICATION_ID = None

RESEARCH_PAPER_STOPWORDS = ['introduction', 'abstract', 'section', 'edition', 'chapter',
                            'copyright', 'preprint', 'figure']

ENGLISH_STOPWORDS =['and', 'the', 'is', 'any', 'to', 'by', 'of', 'on','or', 'with', 'which', 'was','be','we', 'are', 'so',
                    'for', 'it', 'in', 'they', 'were', 'as','at','such', 'no', 'that', 'there', 'then', 'those',
                    'not', 'all', 'this','their','our', 'between', 'have', 'than', 'has', 'but', 'why', 'only', 'into',
                    'during', 'some', 'an', 'more', 'had', 'when', 'from', 'its', "it's", 'been', 'can', 'further',
                    'above', 'before', 'these', 'who', 'under', 'over', 'each', 'because', 'them', 'where', 'both',
                    'just', 'do', 'once', 'through', 'up', 'down', 'other', 'here', 'if', 'out', 'while', 'same',
                    'after', 'did', 'being', 'about', 'how', 'few', 'most', 'off', 'should', 'until', 'will', 'now',
                    'he', 'her', 'what', 'does', 'itself', 'against', 'below', 'themselves','having', 'his', 'am', 'whom',
                    'she', 'nor', 'his', 'hers', 'too', 'own', 'ma', 'him', 'theirs', 'again', 'doing', 'ourselves',
                    're', 'me', 'ours', 'ie', 'you', 'your', 'herself', 'my', 'et', 'al', 'may', 'due', 'de',
                    'one','two', 'three', 'four', 'five','six','seven','eight','nine','ten', 'however',
                    'i', 'ii', 'iii','iv','v', 'vii', 'viii', 'ix', 'x', 'xi', 'xii','xiii', 'xiv']

SIMPLE_STOPWORDS = set(ENGLISH_STOPWORDS + RESEARCH_PAPER_STOPWORDS)