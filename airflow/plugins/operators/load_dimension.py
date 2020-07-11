from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_insert = """
        Insert INTO {}
        {};


        """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt= sql_stmt

    def execute(self, context):
        logging.info('LoadDimensionOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logging.info('LoadDimensionOperator redshift hook connected')

        
        
        redshift.run("DELETE FROM {}".format(self.table))
        logging.info('Cleared destination table {}'.format(self.table))
                                  
        formatted_sql = LoadDimensionOperator.sql_insert.format(
            self.table, 
            self.sql_stmt)
        
        redshift.run(formatted_sql)       
        logging.info('LoadDimensionOperator completed')
        
        
