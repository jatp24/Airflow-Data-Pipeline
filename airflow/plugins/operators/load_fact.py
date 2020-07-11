from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):
    sql_insert = """
    Insert INTO {}
    {};
    
    
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 replace = "replace",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt= sql_stmt
        self.replace = replace

    def execute(self, context):
        logging.info('LoadFactOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logging.info('LoadFactOperator redshift hook connected')

        
        if self.replace.lower() == 'replace':
            redshift.run("DELETE FROM {}".format(self.table))
            logging.info('Cleared destination table {}'.format(self.table))
        elif self.replace.lower() == 'append':
            logging.info('Will append to destination table {}'.format(self.table))
                                                 
                                  
        formatted_sql = LoadFactOperator.sql_insert.format(
            self.table, 
            self.sql_stmt)
        
        redshift.run(formatted_sql)       
   
        logging.info('LoadFactOperator completed')
