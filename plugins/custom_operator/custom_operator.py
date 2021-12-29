from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



class S3toLocalFSOperator(BaseOperator):

    def __init__(self, bucket_name, key_name, file_path, s3_conn_id, *args, **kwargs):
        super(S3toLocalFSOperator, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.key_name = key_name
        self.file_path = file_path
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        print(f"Downloading {self.bucket_name}{self.key_name} to {self.file_path}")

        hook = S3Hook(self.s3_conn_id)

        if hook.check_for_key(self.key_name, self.bucket_name):
            s3_file_object = hook.get_key(self.key_name, self.bucket_name)
            s3_file_list = hook.list_keys(self.bucket_name)
            s3_file_prefix = hook.list_prefixes(self.bucket_name)
            print("obj:", s3_file_object)
            print("file list:", s3_file_list)
            print("prefix:", s3_file_prefix)
            # s3_file_object.download_file(self.bucket_name, self.key_name, self.file_path)
        else:
            print(context)
            raise ValueError('No file exists in S3')

        print('Donwload successful')