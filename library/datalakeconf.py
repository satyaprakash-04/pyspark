import boto3
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
AWS_REGION = os.environ['AWS_REGION']
BUCKET = os.environ['BUCKET']


def connect_s3():
    """This function returns the s3 connection object"""

    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    return s3_client


class InsertDataToDataLake:
    """This class provides basic functionalities to add data to s3 of csv objects.
    It Creates data lake in the structure of
    'module/YYYY/MM/DD/folder_name/file_name_timestamp.csv'
    """
    now = datetime.now()

    def __init__(
            self,
            module,
            file_buffer,
            folder_name,
            file_name,
            file_type='csv'
    ):
        self.module = module
        self.file_buffer = file_buffer
        self.folder_name = folder_name
        self.file_name = file_name
        self.file_type = file_type

    def create_file_path(self):
        date_str = self.now.strftime(format='%Y/%m/%d')
        file_structure = '{}/{}/{}'.format(self.module, date_str, self.folder_name)
        return file_structure

    def create_file_name(self):
        return '{}_{}'.format(
            self.file_name,
            self.now.strftime(format='%Y%m%d%H%M%S%f')
        )

    def generate_file(self):
        return '{}{}.{}'.format(self.create_file_path(), self.create_file_name(), self.file_type)

    def push_data(self):
        s3_client = connect_s3()
        _ = s3_client.put_object(
            Bucket=BUCKET,
            Key=self.generate_file(),
            Body=self.file_buffer
        )


