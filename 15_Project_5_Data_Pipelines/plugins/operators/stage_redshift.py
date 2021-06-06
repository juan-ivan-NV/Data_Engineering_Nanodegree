from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Copyng staging tables data from S3 to redshift
    """
    
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    copy_sql = """
            copy {} FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            COMPUPDATE OFF STATUPDATE OFF
            FORMAT AS JSON '{}'
        """
    
    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_path = "auto", 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Deleting data from the destination Redshift table ...")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Populating table with data from S3 to Redshift")
        key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, key)
        
        sql_format = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshift.run(sql_format)
