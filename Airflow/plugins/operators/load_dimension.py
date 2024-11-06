from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'  

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",  
                 sql_query="",          
                 *args, **kwargs):     
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        # Initialize a PostgresHook to connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Log the action of loading data
        self.log.info('Loading data from staging to dimension table.')
        # Execute the SQL query to load data into the dimension table
        redshift.run(self.sql_query)
        # Log successful completion of the loading process
        self.log.info('Data loading complete.')
