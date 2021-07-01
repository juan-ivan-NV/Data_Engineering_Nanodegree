from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CopyToRedshiftOperator(BaseOperator):
    """Custom Operator for loading data into fact tables.
    
    Attributes:
        ui_color (str): color code for task in Airflow UI.
        template_fields (:obj:`tuple` of :obj: `str`): list of template parameters.
        copy_sql (str): template string for coping data from S3.   
        csv (str): csv formatting template string.
        parq (str): parquet formatting template string.
    """
    ui_color = '#426a87'
    copy_sql = """
        COPY {}
        FROM '{}'
    """
    csv = """
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    IGNOREHEADER {}
    DELIMITER '{}'
    CSV
    """
    parq = """
    IAM_ROLE '{}'
    FORMAT AS PARQUET
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="csv",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):
        """
        Copies csv contents to a Redshift table
        """
        super(CopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table                                            # Name of table to quality check.
        self.redshift_conn_id = redshift_conn_id                      # Airflow connection ID for redshift database.
        self.aws_credentials_id = aws_credentials_id                  # Airlflow connection ID for aws key and secret.
        self.s3_bucket=s3_bucket,                                     # S3 bucket name.
        self.s3_key=s3_key,                                           # S3 Key name.
        self.file_format = file_format                                # format of file to copy to database.
        self.delimiter = delimiter                                    # csv delimiter.
        self.ignore_headers = ignore_headers                          # if to ignore csv headers or not.

    def execute(self, context):
        """Executes task for staging to redshift.
        Args:
            context (:obj:`dict`): Dict with values to apply on content.
        Returns:
            None, executes the data loading
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info(credentials)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        self.log.info('Clearing data from {}'.format(self.table))
        redshift.run('DELETE FROM {}'.format(self.table))
            
        self.log.info('Coping data from {} to {} on table Redshift'.format(s3_path, self.table))    # to log info 

        formatted_sql = CopyToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path
        ) 
        
        """ If the file comes in CSV formatt """
        if self.file_format=='csv':
            formatted_sql += CopyToRedshiftOperator.csv.format(
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        else:
            self.log.info("No CSV formatt found for: {}".format(self.s3_key))
        
        self.log.info('Running Copy Command {}'.format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info('Successfully Copied data from {} to {} table on Redshift'.format(s3_path, self.table))