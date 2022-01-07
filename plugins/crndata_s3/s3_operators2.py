from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3IngestOperator2(BaseOperator):
    """
    S3IngestOperator is an Airflow operator to copy incoming files in S3 for processing. It copies them to
    ingest area after backing up and deleting the input file based on inputs.
    :param aws_connection: The AWS connection name configured in Airflow
    :param input_bucket: The source file bucket
    :param ingest_bucket: The bucket for ingesting into
    :param ingest_prefix: Prefix for the incoming files under ingestion bucket
    :param input_prefix: Default='',
    :param received_file_prefix: where incoming files are backed up after copy. Default='received/',
    :param file_extension: Default='.csv',
    :param delete_after: Whether incoming files should be deleted at source. Default=True,
    """

    # template_fields = [], template_ext = [] - should be a list form.
    template_fields = ['input_bucket', 'input_prefix', 'ingest_bucket', 'ingest_prefix', 'received_file_prefix']
    template_ext = []
    ui_color = '#ededed'

    # I moved delimiter as part of __init__() method's parameter
    # delimiter = '/'

    @apply_defaults
    def __init__(self,
                 task_id,
                 aws_connection,
                 input_bucket,
                 ingest_bucket,
                 ingest_prefix,
                 input_prefix='',
                 delimiter='/',
                 ignore_file_pattern=[], # file_patterns to be skipped in s3_Ingestion task
                 received_file_prefix='received/',
                 file_extension='.csv',
                 delete_after=True,
                 *args, **kwargs):
        super(S3IngestOperator, self).__init__(task_id=task_id, *args, **kwargs)

        self.aws_connection = aws_connection
        self.input_bucket = input_bucket
        self.ingest_bucket = ingest_bucket
        self.ingest_prefix = ingest_prefix
        self.input_prefix = input_prefix
        self.delimiter = delimiter
        self.ignore_file_pattern = ignore_file_pattern
        self.received_file_prefix = received_file_prefix
        self.file_extension = file_extension if file_extension != '*' and file_extension is not None else ''
        self.delete_after = delete_after

    def execute(self, context):
        # https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html
        s3_hook = S3Hook(aws_conn_id=self.aws_connection, verify=False)

        s3_keys = s3_hook.list_keys(bucket_name=self.input_bucket, prefix=self.input_prefix,
                                    delimiter=self.delimiter)

        # len(s3_keys) will return number of files plus directory itself. So, the correct length will be len(s3_keys) - 1
        self.log.info(f"Received {len(s3_keys)} keys from {self.input_bucket}/{self.input_prefix}")

        key: str = ''
        for key in s3_keys:

            if key.endswith(self.delimiter):
                continue

            if not key.lower().endswith(self.file_extension):
                self.log.warning(
                    f"WARNING: Ignoring unknown format file in incoming files folder \
                    {self.ingest_bucket}/{self.ingest_prefix}/{key}")
                continue

            if self.ignore_file_pattern:
                for pattern in self.ignore_file_pattern:
                    filename = self.key_to_file(key)
                    if filename.lower().startswith(pattern):
                        self.log.warning(f'Ignoring because file name starts with "{pattern}" in \
                                        {key}')
                        continue

            filename = self.key_to_file(key)
            self.log.info(f"Processing files from {self.input_bucket}/{key} to \
                {self.ingest_bucket}/{self.ingest_prefix + filename}")
            s3_hook.copy_object(
                source_bucket_key=key,
                source_bucket_name=self.input_bucket,
                dest_bucket_key=self.ingest_prefix + filename,
                dest_bucket_name=self.ingest_bucket
            )
            # Backup source
            if self.received_file_prefix.strip():
                s3_hook.copy_object(
                    source_bucket_key=key,
                    source_bucket_name=self.input_bucket,
                    dest_bucket_name=self.input_bucket,
                    dest_bucket_key=self.received_file_prefix + key
                )

        if self.delete_after:
            self.log.info(f"Deleting objects in bucket {self.input_bucket}: {s3_keys}")
            s3_hook.delete_objects(self.input_bucket, s3_keys)

    def key_to_file(self, key: str):
        """
        Get the filename part converting extension to a standard case
        """

        f = key.rpartition(self.delimiter)[2]
        ext_len = len(self.file_extension)
        filename = f if ext_len < 1 else f[:-ext_len] + self.file_extension
        return filename
