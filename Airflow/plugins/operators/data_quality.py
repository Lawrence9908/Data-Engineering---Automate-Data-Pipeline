from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'  

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,  
                 tables,            
                 *args, **kwargs):  
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Iterate over each table provided for data quality checks
        for table in self.tables:
            self.log.info(f'Starting data quality check on table: {table}')
            # Execute a SQL query to count the number of rows in the table
            records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')
            # Check if the result is valid and if the count is greater than 0
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                raise ValueError(f'Data quality check failed: {table} contains 0 rows.')
            else:
                self.log.info(f'Data quality check passed: {table} contains {records[0][0]} rows.')
