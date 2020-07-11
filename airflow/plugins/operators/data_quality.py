from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    sql_count = """
    SELECT COUNT(*) 
    FROM {}
    """


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables


    def execute(self, context):
        logging.info('DataQualityOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logging.info('DataQualityOperator redshift hook connected')

        
        
        for table in self.tables:
            logging.info("Starting with {}".format(table))
            formatted_sql = DataQualityOperator.sql_count.format(table)

            records = redshift.get_records(formatted_sql) 
            logging.info("Acquired Records for table {}".format(table))
            if len(records) <1 or len(records[0]) <1:
                logging.info("No records in table {}".format(self.table))
                raise ValueError("No records in present destination table {}".format(self.table))
            else:
                print("{} has {} rows".format(table, len(records[0])))
        logging.info('DataQualityOperator is Complete')