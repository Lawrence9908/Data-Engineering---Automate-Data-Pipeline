from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'  

    template_fields = ('s3_key',)  

    copy_sql = """
        COPY {} 
        FROM '{}' 
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",       
                 aws_credentials_id="",      
                 table="",                   
                 s3_bucket="",               
                 s3_key="",                  
                 log_json_file="",           
                 *args, **kwargs):          
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Retrieve AWS credentials from the metastore
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(self.aws_credentials_id)

        # Initialize a PostgresHook to connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data from the destination table
        self.log.info("Clearing data from destination Redshift table: %s", self.table)
        redshift.run(f"TRUNCATE TABLE {self.table}")

        # Prepare to copy data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        # Construct the SQL COPY command
        if self.log_json_file:
            self.log_json_file = f"s3://{self.s3_bucket}/{self.log_json_file}"
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                self.log_json_file
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                aws_connection.login,
                aws_connection.password,
                'auto'
            )

        # Execute the COPY command
        redshift.run(formatted_sql)
        self.log.info("Successfully copied data to Redshift table: %s", self.table)
